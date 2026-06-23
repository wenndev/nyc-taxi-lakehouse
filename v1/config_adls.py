# Databricks notebook source
conta_armazenamento = "pipelinelakehouse"
container = "bronze"  # Bronze, depois muda para silver/gold conforme camada
# Chave da conta (exposta apenas porque é trial e não consigo usar secrets scope)
# account_key = dbutils.secrets.get(scope="adls-scope", key="account-key")
account_key = "SUA_ACCOUNT_KEY_AQUI"

# Configurando Spark para acessar o ADLS via ABFSS
spark.conf.set(
    f"fs.azure.account.key.{conta_armazenamento}.dfs.core.windows.net",
    account_key
)

# gera o caminho correto do data lake, pra não precisa repetir.
def obter_caminho(container_name: str, fonte: str, camada: str, ano: int = 2024):
    base = f"abfss://{container_name}@{conta_armazenamento}.dfs.core.windows.net"
    fonte = fonte.lower()
    camada = camada.lower()

    if fonte == "nyc":
        caminho_fonte = "nyc_tlc/yellow"
    elif fonte == "noaa":
        caminho_fonte = "noaa/weather"
    elif fonte == "analytics":
        caminho_fonte = "analytics"
    else:
        raise ValueError(f"Fonte inválida: {fonte}")

    # Bronze ainda pode ter curinga
    if camada == "bronze":
        return f"{base}/{caminho_fonte}/{ano}/*"
    elif camada == "bronze_delta":
        return f"{base}/{caminho_fonte}/{ano}_delta/"

    # Silver/Gold/dim_date
    elif camada in ["silver", "silver_delta", "gold", "gold_delta", "dim_date"]:
        return f"{base}/{caminho_fonte}/{camada.replace('_delta','')}/{ano}/"

    else:
        raise ValueError(f"Camada inválida: {camada}")