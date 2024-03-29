import traceback

import arrow
import requests


class IntegrationTransactionLog(object):
    _tag = None
    _meta = None
    _id = None
    _end_time = None
    _start_time = None
    _response_url = None
    _exceptions = []
    _exception_count = 0

    _is_error = False

    _exception_handler_func = None
    _logger_func = None

    def __init__(self, sync_type, vendor, credential_id, meta=None):
        self._start_time = arrow.now().isoformat()
        self._tag = IntegrationLoggingService.generate_transaction_tag(sync_type, vendor, credential_id)
        self._meta = meta or dict()

    def set_http_error_handler(self, exception_handler_func):
        self._log('Set exception handler on integration log')
        self._exception_handler_func = exception_handler_func

    def set_logger(self, logger_func):
        self._logger_func = logger_func

    def set_meta_field(self, key, value):
        self._log('set meta field on integration log: {}:{}'.format(key, value))
        self._meta[key] = value

    def create(self):
        self._log('Creating transaction...')

        try:
            response = IntegrationLoggingService.create_transaction(self._tag, self._start_time, self._meta)
        except Exception as e:
            self._log('Error creating transaction: {}'.format(e.args[0]))
            self._is_error = True
            self._on_exception(e)

            return

        self._log('created transaction: {}'.format(response['integration_transaction_id']))
        self._id = response['integration_transaction_id']

    def update(self, end_time=None, meta=None, response_url=None):
        self._log('Updating transaction: {}, {}, {}'.format(end_time, meta, response_url))

        if end_time is not None:
            self._end_time = end_time

        if meta is not None:
            self._meta.update(meta)
            self._log('updated meta')

        self._meta['error_count'] = self._exception_count

        if response_url is not None:
            self._response_url = response_url

        if self._id is None and not self._is_error:
            self._log('Creating transaction since it hasn\'t been created')
            self.create()
        elif self._is_error:
            self._log('Transaction was in error state, did not create')
        else:
            self._log('Updating log ID: {}'.format(self._id))

        if (end_time or meta or response_url) and self._id is not None:
            try:
                IntegrationLoggingService.update_transaction(
                    integration_transaction_id=self._id,
                    end_time=self._end_time,
                    meta=self._meta,
                    response_url=self._response_url)
            except Exception as e:
                self._log('Failed to update transaction: {}'.format(e.args[0]))
                self._on_exception(e)

                return

    def add_exception(self, e):
        self._log('added exception {}'.format(e.args[0]))

        exception_object = IntegrationLoggingService.generate_transaction_exception_object(e)

        self._exceptions.append(exception_object)
        self._exception_count += 1

    def flush_exceptions(self):
        self._log('flushing exceptions')

        if self._id is None and not self._is_error:
            self.create()

        if len(self._exceptions) == 0 or self._id is None:
            return

        try:
            IntegrationLoggingService.create_transaction_exceptions(self._id, self._exceptions)
        except Exception as e:
            self._log('failed to flush exceptions {}'.format(e.args[0]))
            self._on_exception(e)

            return

        self._exceptions = []

    def _on_exception(self, e):
        self._log('error: {}'.format(e.args[0]))

        if self._exception_handler_func is not None:
            self._exception_handler_func(e)

    def _log(self, message):
        if self._logger_func is not None:
            self._logger_func(message)


class IntegrationLoggingService(object):
    _service_host = None

    session = requests.Session()

    @classmethod
    def initialize(cls, service_host):
        cls._service_host = service_host

    @classmethod
    def create_transaction(cls, tag, start_time, meta=None):
        cls._validate_is_initialized()

        payload = dict(
            start_time=start_time,
            tag=tag,
            meta=meta
        )

        request = cls.session.post('{}/transaction'.format(cls._service_host), json=payload)
        request.raise_for_status()

        return request.json()

    @classmethod
    def update_transaction(cls, integration_transaction_id, end_time=None, meta=None, response_url=None):
        cls._validate_is_initialized()

        payload = dict()

        if end_time is not None:
            payload['end_time'] = end_time

        if meta is not None:
            payload['meta'] = meta

        if response_url is not None:
            payload['response_url'] = response_url

        response = cls.session.put('{}/transaction/{}'.format(cls._service_host, integration_transaction_id), json=payload)
        response.raise_for_status()

    @classmethod
    def get_transaction(cls, integration_transaction_id):
        cls._validate_is_initialized()

        response = cls.session.get('{}/transaction/{}'.format(cls._service_host, integration_transaction_id))
        response.raise_for_status()

        return response.json()

    @classmethod
    def search_transactions(cls):
        cls._validate_is_initialized()

        query = dict(distinct='tag', order_by='tag,start_time')

        response = cls.session.post('{}/transaction/search'.format(cls._service_host), json=query)
        response.raise_for_status()

        return response.json()

    @classmethod
    def create_transaction_exceptions(cls, integration_transaction_id, exceptions):
        cls._validate_is_initialized()

        payload = dict(
            exceptions=exceptions
        )

        response = cls.session.post('{}/transaction/{}/exception'.format(cls._service_host, integration_transaction_id), json=payload)
        response.raise_for_status()

    @classmethod
    def _validate_is_initialized(cls):
        if not cls._service_host:
            raise Exception('Not initialized. Call initialize with the service host.')

    @classmethod
    def generate_transaction_exception_object(cls, exception):
        stacktrace = traceback.format_exc()

        return dict(
            message=exception.args[0],
            stack_trace=stacktrace,
            created_time=arrow.now().isoformat()
        )

    @classmethod
    def generate_transaction_tag(cls, sync_type, vendor, credential_id):
        return u'{}-{}-{}'.format(sync_type, vendor, credential_id)
