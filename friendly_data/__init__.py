def logger_config(lvl: int = 0, fmt: str = ""):
    from logging import Formatter, getLogger, StreamHandler, WARNING

    lvl = WARNING if lvl == 0 else lvl
    fmt = fmt if fmt else "{asctime}:{levelname}:{name}:{lineno}: {message}"

    logger = getLogger(__name__)
    formatter = Formatter(fmt, style="{", datefmt="%Y-%m-%dT%H:%M")
    logstream = StreamHandler()
    logstream.setFormatter(formatter)
    logger.addHandler(logstream)
    logger.setLevel(lvl)
    return logger
