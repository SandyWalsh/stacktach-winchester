import abc
import datetime
import logging
import six
import uuid

logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class PipelineHandlerBase(object):
    """Base class for Pipeline handlers.

       Pipeline handlers perform the actual processing on a set of events
       captured by a stream. The handlers are chained together, each handler
       in a pipeline is called in order, and receives the output of the previous
       handler.

       Once all of the handlers in a pipeline have successfully processed the
       events (with .handle_events() ), each handler's .commit() method will be
       called. If any handler in the chain raises an exception, processing of
       events will stop, and each handler's .rollback() method will be called."""

    def __init__(self, **kw):
       """Setup the pipeline handler.

          A new instance of each handler for a pipeline is used for each
          stream (set of events) processed.

          :param kw: The parameters listed in the pipeline config file for
                     this handler (if any).
        """

    @abc.abstractmethod
    def handle_events(self, events, env):
        """ This method handles the actual event processing.

            This method receives a list of events and should return a list of
            events as well. The return value of this method will be passed to
            the next handler's .handle_events() method. Handlers can add new
            events simply by adding them to the list they return. New events
            (those with unrecognized message_id's), will be saved to the
            database if all handlers in this pipeline complete successfully.
            Likewise, handlers can omit events from the list they return to
            act as a filter for downstream handlers.

            Care should be taken to avoid any operation with side-effects in
            this method. Pipelines can be re-tried if a handler throws an
            error. If you need to perform such operations, such as interacting
            with an external system, save the needed information in an instance
            variable, and perform the operation in the .commit() method.

            :param events: A list of events.
            :param env:  Just a dictionary, it's passed to each handler, and
                         can act as a shared scratchpad.

            :returns: A list of events.
        """

    @abc.abstractmethod
    def commit(self):
        """ Called when each handler in this pipeline has successfully
            completed.

            If you have operations with side effects, preform them here.
            Exceptions raised here will be logged, but otherwise ignored.
        """

    @abc.abstractmethod
    def rollback(self):
        """ Called if there is an error for any handler while processing a list
        of events.

        If you need to perform some kind of cleanup, do it here.
        Exceptions raised here will be logged, but otherwise ignored.
        """


class LoggingHandler(PipelineHandlerBase):

    def handle_events(self, events, env):
        emsg = ', '.join("%s: %s" % (event['event_type'], event['message_id'])
                        for event in events)
        logger.info("Received %s events: \n%s" % (len(events), emsg))
        return events

    def commit(self):
        pass

    def rollback(self):
        pass


class UsageException(Exception):
    pass


class UsageHandler(PipelineHandlerBase):
    def _find_exists(self, events):
        exists = None
        for event in events:
            if event['event_type'] == 'compute.instance.exists':
                exists = event
                break

        if not exists:
            raise UsageException("Stream ID %s has no .exists record" %
                                    self.stream_id)

        return exists

    def _extract_launched_at(self, exists):
        if not exists['launched_at']:
            raise UsageException(".exists record for Stream ID %s as "
                                 "no launched_at value."
                                  % self.stream_id)
        return exists['launched_at']

    def _find_events_around(self, events, launched_at):
        interesting = ['compute.instance.rebuild.start',
                       'compute.instance.resize.prep.start',
                       'compute.instance.resize.revert.start',
                       'compute.instance.rescue.start',
                       'compute.instance.create.end',
                       'compute.instance.rebuild.end',
                       'compute.instance.resize.finish.end',
                       'compute.instance.resize.revert.end',
                       'compute.instance.rescue.end']

        subset = [event for event in events
                        if event['event_type'] in interesting]

        from_date = launched_at
        to_date = launched_at + datetime.timedelta(seconds=1)

        final_set = [event for event in subset
                         if event.get('launched_at') >= from_date
                         and event.get('launched_at') <= to_date]

        if not final_set:
            raise UsageException("No events to check .exists against "
                                 "in stream %s" % self.stream_id)

        return final_set

    def _find_deleted_events_around(self, events, launched_at):
        interesting = ['compute.instance.delete.end']

        subset = [event for event in events
                        if event['event_type'] in interesting]

        from_date = launched_at
        to_date = launched_at + datetime.timedelta(seconds=1)

        final_set = [event for event in subset
                         if event.get('launched_at') >= from_date
                         and event.get('launched_at') <= to_date]

        return final_set

    def _verify_fields(self, exists, launch, fields):
        for field in fields:
            if field not in exists and field not in launch:
                continue
            if exists[field] != launch[field]:
                raise UsageException("Conflicting '%s' values ('%s' != '%s')"
                                % (field, exists[field], launch[field]))

    def _confirm_delete(self, exists, deleted, fields):
        if exists.get('deleted_at') and not deleted:
            raise UsageException(".exists event has deleted_at, but no "
                                 "matching .delete event found. "
                                 "Stream ID %s" % self.stream_id)

        if not exists.get('deleted_at') and deleted:
            raise UsageException(".deleted events found but .exists has "
                                 "no deleted_at value. "
                                 "Stream ID %s" % self.stream_id)

        if deleted:
            self._verify_fields(exists, deleted[0],  fields)

    def handle_events(self, events, env):
        self.env = env
        self.stream_id = env['stream_id']

        core_fields = ['launched_at',
                       'instance_type_id',
                       'tenant_id',
                       'rax_options',
                       'os_architecture',
                       'os_version',
                       'os_distro']

        delete_fields = ['launched_at', 'deleted_at']

        exists = None
        error = None
        try:
            exists = self._find_exists(events)
            launched_at = self._extract_launched_at(exists)
            deleted = self._find_deleted_events_around(events, launched_at)
            if len(deleted) > 1:
                raise UsageException("Multiple .delete events for stream %s"
                                                % self.stream_id)
            close = self._find_events_around(events, launched_at)
            if len(close) > 1:
                raise UsageException("Multiple usage events for stream %s"
                                                % self.stream_id)

            self._verify_fields(exists, close[0], core_fields)

            self._confirm_delete(exists, deleted, delete_fields)
            event_type = "compute.instance.exists.verified"
        except UsageException as e:
            error = str(e)
            event_type = "compute.instance.exists.failed"

        if exists:
            new_event = {'event_type': event_type,
                         'message_id': str(uuid.uuid4()),
                         'timestamp': datetime.datetime.utcnow(),
                         'stream_id': int(self.stream_id),
                         'instance_id': exists.get('instance_id'),
                         'error': error
                        }
            logger.debug("NEW EVENT: %s" % new_event)
            events.append(new_event)
        return events

    def commit(self):
        pass

    def rollback(self):
        pass

