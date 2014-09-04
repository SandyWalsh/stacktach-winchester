from contextlib import contextmanager
import logging
import sqlalchemy
from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from winchester import models
from winchester.config import ConfigManager, ConfigSection, ConfigItem


logger = logging.getLogger(__name__)

ENGINES = dict()
SESSIONMAKERS = dict()


class DuplicateError(models.DBException):
    pass


class LockError(models.DBException):
    pass


def sessioned(func):
    def with_session(self, *args, **kw):
        if 'session' in kw:
            return func(self, *args, **kw)
        else:
            with self.in_session() as session:
                kw['session'] = session
                retval = func(self, *args, **kw)
            return retval
    return with_session


class DBInterface(object):


    @classmethod
    def config_description(cls):
        return dict(url=ConfigItem(required=True,
                                       help="Connection URL for database."),
                   )

    def __init__(self, config):
        self.config = ConfigManager.wrap(config, self.config_description())
        self.db_url = config['url']
        self.echo_sql = config.get('echo_sql', False)

    @property
    def engine(self):
        global ENGINES
        if self.db_url not in ENGINES:
            engine = sqlalchemy.create_engine(self.db_url, echo=self.echo_sql)
            ENGINES[self.db_url] = engine
        return ENGINES[self.db_url]

    @property
    def sessionmaker(self):
        global SESSIONMAKERS
        if self.db_url not in SESSIONMAKERS:
            maker = sessionmaker(bind=self.engine)
            SESSIONMAKERS[self.db_url] = maker
        return SESSIONMAKERS[self.db_url]

    def close(self):
        if self.db_url in ENGINES:
            del ENGINES[self.db_url]
        if self.db_url in SESSIONMAKERS:
            del SESSIONMAKERS[self.db_url]

    def get_session(self):
        return self.sessionmaker(expire_on_commit=False)

    @contextmanager
    def in_session(self):
        """Provide a session scope around a series of operations."""
        session = self.get_session()
        try:
            yield session
            session.commit()
        except IntegrityError:
            session.rollback()
            raise DuplicateError("Duplicate unique value detected!")
        except:
            session.rollback()
            raise
        finally:
            session.close()

    @sessioned
    def get_event_type(self, description, session=None):
        t = session.query(models.EventType).filter(models.EventType.desc == description).first()
        if t is None:
            t = models.EventType(description)
            session.add(t)
        return t

    @sessioned
    def create_event(self, message_id, event_type, generated, traits, session=None):
        event_type = self.get_event_type(event_type, session=session)
        e = models.Event(message_id, event_type, generated)
        for name in traits:
            e[name] = traits[name]
        session.add(e)

    @sessioned
    def get_event_by_message_id(self, message_id, session=None):
        e = session.query(models.Event).\
            filter(models.Event.message_id == message_id).one()
        return e.as_dict

    @sessioned
    def get_stream_by_id(self, stream_id, session=None):
        s = session.query(models.Stream).\
            filter(models.Stream.id == stream_id).one()
        return s

    @sessioned
    def create_stream(self, trigger_name, initial_event, dist_traits, expire_expr, session=None):
        first_event_time = initial_event['timestamp']
        s = models.Stream(trigger_name, first_event_time)
        for trait_name in dist_traits:
            s[trait_name] = dist_traits[trait_name]
        session.add(s)
        self.add_event_stream(s, initial_event, expire_expr, session=session)
        return s

    @sessioned
    def stream_has_dist_trait(self, stream_id, name, value=None, session=None):
        q = session.query(models.DistinguishingTrait)
        q = q.filter(models.DistinguishingTrait.stream_id == stream_id)
        q = q.filter(models.DistinguishingTrait.name == name)
        if value is not None:
            q = q.filter(models.DistinguishingTrait.value == value)
        dt = q.first()
        if dt is not None:
            dt = dt.as_dict
        return dt

    @sessioned
    def get_stream_events(self, stream, session=None):
        if stream not in session:
            stream = session.merge(stream)
        return [event.as_dict for event in stream.events]

    @sessioned
    def add_event_stream(self, stream, event, expire_expr, session=None):
        if stream not in session:
            session.add(stream)
        message_id = event['message_id']
        timestamp = event['timestamp']
        if timestamp < stream.first_event:
            stream.first_event = timestamp
        if timestamp > stream.last_event:
            stream.last_event = timestamp
        stream.expire_timestamp = expire_expr(first=stream.first_event,
                                              last=stream.last_event).timestamp
        eq = session.query(models.Event)
        eq = eq.filter(models.Event.message_id == message_id)
        e = eq.one()
        stream.events.append(e)
        return e

    @sessioned
    def get_active_stream(self, name, dist_traits, current_time, session=None):
        q = session.query(models.Stream)
        q = q.filter(models.Stream.name == name)
        q = q.filter(models.Stream.state == int(models.StreamState.active))
        q = q.filter(models.Stream.expire_timestamp > current_time)
        for name, val in dist_traits.items():
            q = q.filter(models.Stream.distinguished_by.any(and_(
                    models.DistinguishingTrait.name == name,
                    models.DistinguishingTrait.value == val)))
        return q.first()

    @sessioned
    def stream_ready_to_fire(self, stream, timestamp, session=None):
        if stream not in session:
            session.add(stream)
        stream.fire_timestamp = timestamp

    @sessioned
    def get_ready_streams(self, batch_size, current_time, expire=False, session=None):
        q = session.query(models.Stream)
        q = q.filter(models.Stream.state == int(models.StreamState.active))
        if expire:
            q = q.filter(models.Stream.expire_timestamp < current_time)
        else:
            q = q.filter(models.Stream.fire_timestamp < current_time)
        q = q.limit(batch_size)
        return q.all()

    def set_stream_state(self, stream, state):
        serial = stream.state_serial_no
        stream_id = stream.id
        #we do this in a separate session, as it needs to be atomic.
        with self.in_session() as session:
            q = session.query(models.Stream)
            q = q.filter(models.Stream.id == stream_id)
            q = q.filter(models.Stream.state_serial_no == serial)
            ct = q.update(dict(state=int(state), state_serial_no=serial + 1))
        if ct != 1:
            raise LockError("Optimistic Lock failed!")
        return self.get_stream_by_id(stream_id)
