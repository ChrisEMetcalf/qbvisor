# Transport

Most applications should let `QuickBaseClient` create and close its transport. Supply an explicit
transport when you need a caller-owned `requests.Session`, custom timeouts, or controlled retry
timing.

::: qbvisor.transport.QuickBaseTransport
    options:
      members:
        - close
        - get
        - get_bytes
        - get_file
        - post
        - delete

::: qbvisor.transport.RetryPolicy
