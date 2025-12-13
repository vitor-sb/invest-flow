import os
import time
import glob
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

URL = 'https://statusinvest.com.br/acoes/busca-avancada'
NOME_TABELA_ALVO = 'acoes_screener_magic_number'

DATA_DIR = '/opt/airflow/data'
RAW_CSV_PATH = os.path.join(DATA_DIR, 'status_invest_raw.csv')
CLEAN_PARQUET_PATH = os.path.join(DATA_DIR, 'status_invest_clean.parquet')


def setup_selenium_driver(download_dir):
    """Configura o driver do Chrome para rodar no Docker (headless)."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    chrome_options = Options()

    chrome_options.binary_location = "/usr/bin/chromium"

    # --- INÍCIO DA CAMUFLAGEM ---
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    chrome_options.add_argument(f"user-agent={user_agent}")

    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    chrome_options.add_argument("--headless=new")
    # --- FIM DA CAMUFLAGEM ---

    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=Service(),
                              options=chrome_options)
    return driver


@dag(
    dag_id='status_invest_screener_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['portfolio', 'scraping', 'selenium']
)
def status_invest_screener_pipeline():
    """
    Pipeline que extrai dados de Ações do Status Invest,
    transforma e salva em um Data Warehouse Postgres.
    """

    @task
    def setup_directories():
        """Garante que o diretório de dados existe."""
        os.makedirs(DATA_DIR, exist_ok=True)
        if os.path.exists(RAW_CSV_PATH):
            os.remove(RAW_CSV_PATH)
        if os.path.exists(CLEAN_PARQUET_PATH):
            os.remove(CLEAN_PARQUET_PATH)

    @task
    def extract_status_invest_data() -> str:
        """
        Usa Selenium para baixar o CSV do Status Invest.
        Retorna o caminho do arquivo baixado.
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException

        log = logging.getLogger(__name__)
        driver = setup_selenium_driver(DATA_DIR)
        wait = WebDriverWait(driver, 15)
        wait_curto = WebDriverWait(driver, 7)

        def close_known_popups():
            """
            Função auxiliar que tenta fechar todos os popups conhecidos.
            Sua sugestão de 'limpar' a tela.
            """
            log.info("Procurando por popups conhecidos para fechar...")
            popups_closed = 0

            # Lista de seletores de BOTÕES DE FECHAR
            close_selectors = [
                (By.XPATH, "//div[@id='footer-fixed']/a"),  # 1. Footer Banner
                (By.XPATH, '//button[contains(@class, "wisepops-close")]'),  # 2. Wisepops Modal
                # 3. Popup Genérico (o que causou o erro, como visto no log/screenshot)
                (By.XPATH, "//div[contains(@class, 'popup-fixed')]//button[contains(@class, 'btn-close')]")
            ]

            for (by, selector) in close_selectors:
                try:
                    # Usar find_elements para não quebrar se não existir
                    elements = driver.find_elements(by, selector)
                    for el in elements:
                        if el.is_displayed():
                            log.info(f"Tentando fechar popup: {selector}")
                            driver.execute_script("arguments[0].click();", el)
                            popups_closed += 1
                            time.sleep(0.5)
                except Exception as e:
                    log.warning(f"Erro ao tentar fechar popup {selector}: {e.__class__.__name__}")

            if popups_closed > 0:
                log.info(f"{popups_closed} popup(s) fechado(s).")

        DEBUG_SCREENSHOT_PATH = os.path.join(DATA_DIR, 'debug_screenshot.png')

        try:
            log.info(f'Fazendo requisição para: {URL}')
            driver.get(URL)
            time.sleep(1)  # Espera inicial

            # --- ETAPA 1: Limpar popups iniciais ---
            close_known_popups()

            # --- ETAPA 2: SCROLL e CLIQUE no "Buscar" ---
            log.info('Aguardando botão "Buscar"...')
            seletor_buscar = '//button[contains(@class, "find")]'
            button_buscar = wait.until(EC.presence_of_element_located((By.XPATH, seletor_buscar)))
            driver.execute_script("arguments[0].scrollIntoView(true);", button_buscar)
            time.sleep(0.5)

            # Tenta fechar popups DE NOVO, caso apareçam após o scroll
            close_known_popups()

            wait.until(EC.element_to_be_clickable((By.XPATH, seletor_buscar))).click()
            log.info("Botão 'Buscar' clicado.")

            # --- ETAPA 3: Espera Tabela e Fecha Popups Pós-Busca ---
            wait.until(EC.visibility_of_element_located((By.ID, "list-result")))
            log.info("Tabela de resultados visível.")

            log.info("Procurando popups pós-busca (antes do download)...")
            time.sleep(1)
            close_known_popups()

            # --- ETAPA 4: Clicar em "Download" ---
            log.info("Aguardando botão 'Download'...")
            seletor_download = '//a[contains(@class, "btn-download")]'

            download_button = wait.until(EC.element_to_be_clickable((By.XPATH, seletor_download)))
            driver.execute_script("arguments[0].click();", download_button)
            log.info("Comando de 'Download' enviado.")

            # --- ETAPA 5: ESPERA DE DOWNLOAD ROBUSTA ---
            tempo_limite = 30
            tempo_inicial = time.time()
            arquivo_encontrado = False

            while time.time() - tempo_inicial < tempo_limite:
                if glob.glob(os.path.join(DATA_DIR, "*.crdownload")):
                    time.sleep(1)
                    continue

                arquivos_csv = glob.glob(os.path.join(DATA_DIR, "*.csv"))
                if arquivos_csv:
                    caminho_arquivo_real = arquivos_csv[0]
                    log.info(f"Download concluído. Arquivo: {caminho_arquivo_real}")
                    os.rename(caminho_arquivo_real, RAW_CSV_PATH)
                    log.info(f"Arquivo renomeado para: {RAW_CSV_PATH}")
                    arquivo_encontrado = True
                    break
                time.sleep(1)

            if not arquivo_encontrado:
                raise TimeoutException("Download não concluído no tempo limite.")

        except Exception as e:
            log.error(f"Erro CRÍTICO durante a extração: {e.__class__.__name__}: {e}")
            log.info("Tentando salvar screenshot de debug...")
            try:
                driver.save_screenshot(DEBUG_SCREENSHOT_PATH)
                log.info(f"Screenshot de debug salvo em: {DEBUG_SCREENSHOT_PATH}")
            except Exception as e_shot:
                log.error(f"Falha ao salvar o screenshot: {e_shot}")
            raise

        finally:
            log.info("Fechando o driver do Chrome.")
            driver.quit()

        return RAW_CSV_PATH

    @task
    def transform_and_filter_data(raw_csv_file_path: str) -> str:
        """
        Lê o CSV bruto, limpa, aplica filtros e salva em Parquet.
        Retorna o caminho do arquivo Parquet.
        """
        log = logging.getLogger(__name__)
        log.info(f"Lendo dados de stage: {raw_csv_file_path}")

        try:
            df_raw = pd.read_csv(raw_csv_file_path, sep=';', decimal=',', thousands='.')
        except FileNotFoundError:
            log.error(f"Arquivo não encontrado no path: {raw_csv_file_path}")
            raise

        log.info(f"Dados brutos lidos. {df_raw.shape[0]} linhas encontradas.")
        df_clean = df_raw.copy()
        df_clean.columns = df_clean.columns.str.strip()

        METRICAS_CHAVE = ['P/L', 'ROE', 'LIQ. CORRENTE', 'PRECO', 'DY', 'LIQUIDEZ MEDIA DIARIA', 'P/VP']
        for col in METRICAS_CHAVE:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
            else:
                log.warning(f"Aviso: Coluna '{col}' não foi encontrada no CSV.")

        log.info("Aplicando filtros...")
        filtro_pl = (df_clean['P/L'] > 3) & (df_clean['P/L'] < 10)
        filtro_dy = (df_clean['DY'] > 6) & (df_clean['DY'] < 15)
        filtro_pvp = (df_clean['P/VP'] > 0.5) & (df_clean['P/VP'] < 2)
        filtro_lmd = (df_clean['LIQUIDEZ MEDIA DIARIA'] > 1000000)

        df_filtrado = df_clean[filtro_pl & filtro_dy & filtro_pvp & filtro_lmd].copy()

        log.info("Calculando DPA, Magic Number e Dividendo Gerado...")
        df_filtrado['DPA (R$)'] = df_filtrado['PRECO'] * (df_filtrado['DY'] / 100)
        df_filtrado['MAGIC_NUMBER'] = np.where(
            df_filtrado['DY'] > 0,
            np.ceil(100 / df_filtrado['DY']),
            np.inf
        )
        df_filtrado['DIVIDENDO_GERADO (R$)'] = df_filtrado['MAGIC_NUMBER'] * df_filtrado['DPA (R$)']
        df_filtrado = df_filtrado.sort_values(by='MAGIC_NUMBER')

        # Adiciona uma coluna de data de carga
        df_filtrado['dt_carga'] = pd.to_datetime('today')

        log.info(f"Salvando {df_filtrado.shape[0]} linhas em Parquet: {CLEAN_PARQUET_PATH}")
        df_filtrado.to_parquet(CLEAN_PARQUET_PATH, index=False)

        return CLEAN_PARQUET_PATH

    @task
    def load_data_to_postgres(clean_parquet_file_path: str):
        """
        Carrega os dados do arquivo Parquet para a tabela no Postgres.
        """
        log = logging.getLogger(__name__)
        log.info(f"Lendo dados limpos de: {clean_parquet_file_path}")
        df = pd.read_parquet(clean_parquet_file_path)

        hook = PostgresHook(postgres_conn_id='postgres_data_warehouse')

        log.info("Obtendo a URI de conexão do Hook...")
        conn_uri = hook.get_uri()

        log.info(f"Carregando {df.shape[0]} linhas na tabela {NOME_TABELA_ALVO}...")

        df.to_sql(
            NOME_TABELA_ALVO,
            con=conn_uri,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )

        log.info("Carga concluída com sucesso.")

    @task
    def cleanup_local_files():
        """Remove os arquivos temporários da área de dados."""
        log = logging.getLogger(__name__)
        if os.path.exists(RAW_CSV_PATH):
            os.remove(RAW_CSV_PATH)
            log.info(f"Arquivo bruto removido: {RAW_CSV_PATH}")
        if os.path.exists(CLEAN_PARQUET_PATH):
            os.remove(CLEAN_PARQUET_PATH)
            log.info(f"Arquivo limpo removido: {CLEAN_PARQUET_PATH}")

    task_setup = setup_directories()
    task_extract = extract_status_invest_data()
    task_transform = transform_and_filter_data(task_extract)
    task_load = load_data_to_postgres(task_transform)
    task_cleanup = cleanup_local_files()

    task_setup >> task_extract >> task_transform >> task_load >> task_cleanup

status_invest_screener_pipeline()