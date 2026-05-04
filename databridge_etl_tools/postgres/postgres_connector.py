import sys
import logging
import psycopg2

class Postgres_Connector():
    '''Represent a Postgres connection. Do not remove **kwargs from class.'''
    def __init__(self, connection_string, **kwargs):
        self.connection_string = connection_string
        self._logger = None
        self._conn = None
        self.conn

    def __enter__(self):
        '''Context manager functions to be called BEFORE any functions inside
        ```
        with Connector(...) as connector:
            ...
        ```
        See https://book.pythontips.com/en/latest/context_managers.html
        '''
        return self

    def __exit__(self, type, value, traceback):
        '''Context manager functions to execute AFTER all functions inside
        ```
        with Connector(...) as connector:
            ...
        ```
        '''
        self.conn.close()
        self.logger.info('Connection closed.\n')

    @property
    def conn(self):
        '''Create or Make the Postgres db connection'''
        if self._conn is None:
            self.logger.info('Trying to connect to postgres...')
            conn = psycopg2.connect(self.connection_string, connect_timeout=5)
            self._conn = conn
            self.logger.info('Connected to postgres.\n')
        return self._conn

    @property
    def logger(self):
        if self._logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
            if logger.handlers == []:
                sh = logging.StreamHandler(sys.stdout)
                logger.addHandler(sh)
            self._logger = logger
        return self._logger
