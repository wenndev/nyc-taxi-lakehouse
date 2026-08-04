from __future__ import annotations


class DataQualityError(RuntimeError):
    pass


class DataQualityCriticalError(DataQualityError):
    pass

