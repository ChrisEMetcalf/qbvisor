# Logging

qbvisor never configures handlers automatically. Applications can use their own logging setup or
call `LoggingConfigurator.setup()` once at startup.

::: qbvisor.log_runner.LoggingConfigurator
    options:
      members:
        - setup

::: qbvisor.log_runner.get_logger
