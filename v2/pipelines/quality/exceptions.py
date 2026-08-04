# Resumo:
# - Define excecoes especificas da camada de Data Quality.
# - Usado para bloquear a publicacao da Silver quando a qualidade retorna FAIL.

from __future__ import annotations


class DataQualityError(RuntimeError):
    pass


class DataQualityCriticalError(DataQualityError):
    pass
