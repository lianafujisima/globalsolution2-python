from datetime import datetime, timedelta
import pandas as pd
import os
import json
import oracledb
from tabulate import tabulate
import requests

margem = ' ' * 4

# Definição de métricas positivas ou negativas
metrica_positiva_negativa = {
    "produtividade": "positivo",
    "foco": "positivo",
    "estresse": "negativo",
    "humor": "positivo",
    "energia": "positivo",
    "controle_dia": "positivo",
    "satisfacao": "positivo",
    "reconhecimento": "positivo",
    "carga_trabalho": "negativo",
    "sono_horas": "positivo",
    "sono_descanso": "positivo",
    "despertares": "negativo",
    "agua": "positivo",
    "atividade_fisica": "positivo"
}
# Mapeamento colunas Colaborador(renomeando)
mapeamento_colunas = {
    'id': 'Id',
    'nr_cpf': 'CPF',
    'nm_colaborador': 'Nome',
    'dt_nascimento': 'Data de nascimento',
    'ds_sexo': 'Sexo',
    'cep': 'CEP',
    'ds_logradouro': 'Logradouro',
    'nr_endereco': 'Número',
    'ds_bairro': 'Bairro',
    'ds_cidade': 'Cidade',
    'ds_estado': 'Estado',
    'vl_salario': 'Salário',
    'ds_cargo': 'Cargo',
    'dt_admissao': 'Data de admissão',
    'dt_demissao': 'Data de demissão',
    'ds_status': 'Status',
    'dt_criacao': 'Data Criação',
    'dt_ultima_modificacao': 'Data última modificação'
}
# Mapeamento colunas Métricas(renomeando)
colunas_renomear ={
    "nr_cpf": "CPF",
    "produtividade": "Produtividade",
    "foco": "Foco",
    "estresse": "Estresse",
    "humor": "Humor",
    "energia": "Energia",
    "controle_dia": "Controle do dia",
    "satisfacao": "Satisfação",
    "relacao_colegas": "Relação colegas",
    "reconhecimento": "Reconhecimento",
    "carga_trabalho": "Carga trabalho",
    "sono_horas": "Sono (horas)",
    "sono_descanso": "Sono (descanso)",
    "despertares": "Despertares",
    "atividade_fisica": "Atividade física",
    "agua": "Água",
    "intensidade_atividade": "Intensidade da atividade",
    "tarefas_concluidas": "Tarefas concluídas",
    "tarefas_andamento": "Tarefas em andamento",
    "tarefas_pendentes": "Tarefas pendentes",
    "concluidas_no_prazo": "Tarefas concluídas no prazo",
    "concluidas_atraso": "Tarefas concluídas com atraso"
}

# ====== AUXILIARES ======

def limpa_tela():
    """
    Limpa a tela do terminal no Windows e no Linux/macOS.
    """    
    os.system('cls' if os.name == 'nt' else 'clear')

def valida_nota(msg: str) -> int:
    """
    Solicita uma nota de 0 a 10 ao usuário e garante que o valor iserido seja válido. 
    Enquanto usuário digitar valor inválido, exibe mensagem de erro e solicita resposta novamente.

    Args:
        msg (str): mensagem exibida ao solicitar a nota.

    Returns:
        int: valor inteiro entre 0 e 10
    """    
    while True:
        valor = input(msg).strip()
        if valor.isdigit() and 0 <= int(valor) <= 10:
            return int(valor)
        print(f"\n {margem} Valor inválido! Digite número inteiro entre 0 e 10.\n")

def validar_cpf(cpf: str) -> bool:
    """
    Verifica se a string fornecida contém exatamente 11 dígitos numéricos.
    A função remove todos os caracteres não numéricos da string cpf.

    Args:
        cpf (str): String contendo um CPF, com ou sem máscara.

    Returns:
        bool: True se o CPF possui 11 dígitos numéricos, False caso contrário.
    """
    cpf_numeros = "".join(c for c in cpf if c.isdigit())
    return len(cpf_numeros) == 11

def cpf_unico(conn: oracledb.Connection, cpf: str) -> bool:
    """
    Verifica se um CPF já está cadastrado na tabela T_MNDSH_COLABORADOR.
    Caso o CPF não exista, retorna True.
    Caso exista já exista, retorna False.

    Args:
        conn (oracledb.Connection): Conexão ativa com o banco de dados Oracle.
        cpf (str): CPF a ser verificado (somente números ou formatado).

    Returns:
        bool: 
            - True se o CPF não estiver cadastrado.
            - False se já existe.
    Raises:
        (Exceção interna do OracleDB): Se houver um erro de conexão
            ou na execução da query, a exceção é capturada e uma
            mensagem de erro é exibida.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM T_MNDSH_COLABORADOR WHERE nr_cpf = :cpf", {"cpf": cpf})
        qtd = cursor.fetchone()[0]
        return qtd == 0
    except Exception as e:
        print(f"\n {margem} Erro ao verificar CPF: {e} \n")
        return False
    finally:
        cursor.close()

def validar_data(data_str: str) -> bool:
    """
    Verifica se uma string representa uma data válida no formato "DD/MM/AAAA".
    A função tenta converter a string de data para um objeto datetime
    usando o formato específico.

    Args:
        data_str: A string que contém a data a ser validada.

    Returns:
        bool: True se a data for válida no formato especificado; False caso contrário.

    Raises:
        Não levanta exceções diretamente; captura a exceção ValueError da função datetime.strptime para retornar False.
    """
    try:
        datetime.strptime(data_str, "%d/%m/%Y")
        return True
    except ValueError:
        return False

def data_datetime(data_str: str) -> datetime:
    """
    Converte uma string de data no formato "DD/MM/AAAA" para um objeto datetime, utilizando strptime.
    É recomendado usar a função validar_data() antes de chamar esta função.

    Args:
        data_str: A string contendo a data a ser convertida.

    Returns:
        datetime: Um objeto datetime correspondente à data fornecida.

    Raises:
        ValueError: Se a string de data não estiver no formato "%d/%m/%Y" ou se representar uma data inválida.
    """
    return datetime.strptime(data_str, "%d/%m/%Y")

def imprimir_tabela(df: pd.DataFrame, titulo: str ="Tabela", tamanhos_wrap: dict | None = None, colunas_datas: list[str] | None = None, colunas_datetime: list[str] | None = None, colunas_moeda: list[str] | None = None, colunas_exibir=None) -> None:
    """
    Formata e imprime um DataFrame do Pandas no console com opções de customização/formatação.

    Args:
        df: O DataFrame do Pandas a ser exibido.
        titulo: O título a ser exibido acima da tabela.
        tamanhos_wrap: Dicionário onde a chave é a coluna e o valor é o tamanho
                       máximo (int) para truncar o conteúdo. Padrão é None.
        colunas_datas: Lista de nomes de colunas para formatar como data (DD/MM/AAAA).
        colunas_datetime: Lista de nomes de colunas para formatar como datetime.
        colunas_moeda: Lista de nomes de colunas para formatar como moeda (R$ X.XXX,XX).
        colunas_exibir: Define quais colunas serão exibidas e como serão agrupadas.
                        Aceita diferentes formatos (lista simples, lista de listas, lista de tuplas).
                        Padrão é None.

    Returns:
        None: A função apenas imprime a saída diretamente no console.
    """
    df_formatado = df.copy()
    if colunas_datas:
        for coluna in colunas_datas:
            if coluna in df_formatado.columns:
                df_formatado[coluna] = pd.to_datetime(df_formatado[coluna], format='%d/%m/%Y', errors='coerce').dt.strftime('%d/%m/%Y')
                df_formatado[coluna] = pd.to_datetime(df_formatado[coluna], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
    if colunas_datetime:
        for coluna in colunas_datetime:
            if coluna in df_formatado.columns:
                df_formatado[coluna] = pd.to_datetime(df_formatado[coluna], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    if colunas_moeda:
        for coluna in colunas_moeda:
            if coluna in df_formatado.columns:
                def formatar_moeda(x):
                    try:
                        return f"R$ {float(x):,.2f}"
                    except (ValueError, TypeError):
                        return ""
                df_formatado[coluna] = df_formatado[coluna].apply(formatar_moeda)
    if tamanhos_wrap:
        for coluna, tamanho in tamanhos_wrap.items():
            if coluna in df_formatado.columns:
                df_formatado[coluna] = df_formatado[coluna].apply(lambda x: str(x)[:tamanho])

    print(f"\n===== {titulo} =====\n")
    if colunas_exibir:
        if isinstance(colunas_exibir[0], tuple) and len(colunas_exibir[0]) == 2 and isinstance(colunas_exibir[0][1], list):
            for bloco_titulo, grupo in colunas_exibir:
                colunas_grupo = df_formatado.columns.intersection(grupo)
                if colunas_grupo.empty:
                    continue

                df_grupo = df_formatado[colunas_grupo]
                
                print(f"--- {bloco_titulo} ---") 
                print(df_grupo.to_string(index=False))
                print("\n")
                
        elif isinstance(colunas_exibir[0], list):
            for i, grupo in enumerate(colunas_exibir):
                colunas_grupo = df_formatado.columns.intersection(grupo)
                if colunas_grupo.empty:
                    continue
                df_grupo = df_formatado[colunas_grupo]
                print(df_grupo.to_string(index=False))
                print("\n")
        else:
             print(df_formatado.to_string(index=False))
    else:
        print(df_formatado.to_string(index=False))
        
def parse_salario(valor: str) -> float | None:
    """
    Converte uma string de salário formatada (R$, pontos, vírgulas) em float.
    A função limpa a string, removendo símbolos de moeda ('R$') e ajustando separadores decimais.

    Args:
        valor: A string contendo o valor do salário.

    Returns:
        float: O valor do salário como número de ponto flutuante.
        None: Se a string de entrada for vazia, não puder ser convertida para um número válido, ou se o valor for zero ou negativo.
    """
    try:
        if not valor:
            return None
        valor = valor.replace("R$", "").replace(".", "").replace(",", ".").strip()
        salario = float(valor)
        if salario <= 0:
            return None
        return salario
    except ValueError:
        return None

def gerar_dataframe(df: pd.DataFrame) -> None:
    """
    Apresenta um menu para salvar um DataFrame em diferentes formatos de arquivo.
    Utiliza a função auxiliar 'menu_opcoes' para solicitar ao usuário o formato de 
    salvamento (CSV, Excel, JSON ou Não salvar) e, em seguida, solicita o nome do arquivo.

    Args:
        df: O DataFrame do Pandas (pd.DataFrame) que contém os dados a serem salvos.

    Returns:
        None: A função realiza a operação de salvamento e não retorna nenhum valor.
    
    Dependências:
        Esta função depende de 'menu_opcoes', 'margem'.
    """
    opcoes_texto = ["CSV", "Excel", "JSON", "Não salvar"]
    opcoes_valor = ["csv", "excel", "json", "nao"]
    escolha = menu_opcoes("\nDeseja salvar os dados?\n\nEscolha o formato:", opcoes_texto, opcoes_valor)
    if escolha == "csv":
        nome_arquivo = input("\nNome do arquivo CSV: ").strip() or "dados.csv"
        if not nome_arquivo.lower().endswith(".csv"):
            nome_arquivo += ".csv"
        df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig')
        print(f"\n {margem} Arquivo CSV salvo como {nome_arquivo}\n")
    elif escolha == "excel":
        nome_arquivo = input("\nNome do arquivo Excel: ").strip() or "dados.xlsx"
        if not nome_arquivo.lower().endswith(".xlsx"):
            nome_arquivo += ".xlsx"
        df.to_excel(nome_arquivo, index=False)
        print(f"\n {margem} Arquivo Excel salvo como {nome_arquivo}\n")
    elif escolha == "json":
        nome_arquivo = input("\nNome do arquivo JSON: ").strip() or "dados.json"
        if not nome_arquivo.lower().endswith(".json"):
            nome_arquivo += ".json"
        df.to_json(nome_arquivo, orient="records", force_ascii=False, indent=4)
        print(f"\n {margem} Arquivo JSON salvo como {nome_arquivo}\n")
    else:
        print(f"\n {margem} Não salvando arquivo.\n")
    input("Pressione ENTER para continuar...")

def perguntar_continuar(acao: str ="tentar novamente") -> bool:
    """
    Solicita a confirmação do usuário para continuar ou repetir uma ação.
    A função exibe um menu de Sim/Não, pausando a execução do programa antes de retornar False (Não).

    Args:
        acao: O texto da ação a ser perguntada. Será exibido como: "Deseja [acao]?". Padrão é "tentar novamente".

    Returns:
        bool: True se o usuário escolher 'Sim' (continuar/repetir). False se o usuário escolher 'Não' (voltar/parar).
              
    Dependências:
        Esta função depende de 'menu_opcoes2', 'margem' e 'limpa_tela'.
    """
    while True:
        escolha = menu_opcoes2(f"\n{margem} Deseja {acao}?\n", ["Sim", "Não"], ["1", "2"])
        if escolha == "1":
            return True
        elif escolha == "2":
            print(f"\n{margem} Voltando...\n")
            input("Pressione ENTER para continuar...")
            limpa_tela()
            return False

def perguntar_continuar2(acao: str ="tentar novamente") -> bool:
    """
    Solicita a confirmação do usuário para continuar ou repetir uma ação.
    A função exibe um menu de Sim/Não. Se o usuário escolher 'Sim', a tela é limpa imediatamente 
    antes de retornar True. Se escolher 'Não', exibe uma mensagem, pausa a execução e limpa a tela.

    Args:
        acao: O texto da ação a ser perguntada. Será exibido como: "Deseja [acao]?". Padrão é "tentar novamente".

    Returns:
        bool: True se o usuário escolher 'Sim' (continuar/repetir). False se o usuário escolher 'Não' (voltar/parar).
              
    Dependências:
        Esta função depende de 'menu_opcoes', 'margem' e 'limpa_tela'.
    """
    while True:
        escolha = menu_opcoes(f"\n{margem} Deseja {acao}?\n", ["Sim", "Não"], ["1", "2"])
        if escolha == "1":
            limpa_tela()
            return True
        elif escolha == "2":
            print(f"\n{margem} Voltando...\n")
            input("Pressione ENTER para continuar...")
            limpa_tela()
            return False

def buscar_colaborador(conn: oracledb.Connection, identificador: str | None = None, titulo_menu: str | None = None)-> dict | None:
    """
    Busca um colaborador no banco de dados Oracle por ID ou CPF.
    Esta função entra em um loop contínuo que solicita ao usuário um identificador, quando válido, 
    valida o formato (ID numérico ou CPF de 11 dígitos) e executa a consulta apropriada na tabela T_MNDSH_COLABORADOR.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.
        identificador: O ID ou CPF do colaborador (string) a ser buscado diretamente. Se for None, o valor é solicitado ao usuário.
        titulo_menu: O título a ser exibido no menu de solicitação.

    Returns:
        dict: Um dicionário contendo os dados do colaborador.
        None: Se o colaborador não for encontrado ou se ocorrer um erro.

    Dependências:
        Esta função depende de 'perguntar_continuar2' e 'margem'.
    """
    cursor = conn.cursor()
    try:
        while True:
            if not identificador:
                print(f"\n===== {titulo_menu} =====\n")
                identificador = input("ID ou CPF do colaborador: ").strip()
            if identificador.isdigit() and len(identificador) != 11:
                cursor.execute("""
                    SELECT id, nr_cpf, nm_colaborador, dt_nascimento, ds_sexo, cep, ds_logradouro, nr_endereco, ds_bairro,
                           ds_cidade, ds_estado, vl_salario, ds_cargo, dt_admissao, dt_demissao, ds_status, dt_criacao, dt_ultima_modificacao
                    FROM T_MNDSH_COLABORADOR
                    WHERE id = :id
                """, {"id": int(identificador)})
            elif identificador.isdigit() and len(identificador) == 11:
                cursor.execute("""
                    SELECT id, nr_cpf, nm_colaborador, dt_nascimento, ds_sexo, cep, ds_logradouro, nr_endereco, ds_bairro,
                           ds_cidade, ds_estado, vl_salario, ds_cargo, dt_admissao, dt_demissao, ds_status,dt_criacao, dt_ultima_modificacao
                    FROM T_MNDSH_COLABORADOR
                    WHERE nr_cpf = :cpf
                """, {"cpf": identificador})
            else:
                print(f"\n {margem} Identificador inválido. Use um ID numérico ou CPF com 11 dígitos.\n")
                if not perguntar_continuar2("tentar novamente"):
                    return None
                identificador = None
                continue
            resultado = cursor.fetchone()
            if resultado:
                colunas = ["id", "nr_cpf", "nm_colaborador", "dt_nascimento", "ds_sexo", "cep", "ds_logradouro", "nr_endereco", "ds_bairro", 
                    "ds_cidade", "ds_estado", "vl_salario", "ds_cargo", "dt_admissao", "dt_demissao", "ds_status", "dt_criacao", "dt_ultima_modificacao"]
                colaborador_dicionario = dict(zip(colunas, resultado))
                for k, v in colaborador_dicionario.items():
                    if v is None:
                        if k in ['vl_salario', 'nr_endereco']:
                            colaborador_dicionario[k] = 0
                        else:
                            colaborador_dicionario[k] = ""
                return colaborador_dicionario
            else:
                print(f"\n {margem} Colaborador não encontrado.\n")
                if not perguntar_continuar2("tentar outro ID ou CPF"):
                    return None
                identificador = None
    except Exception as e:
        print(f"\n{margem} Erro ao buscar colaborador: {e}\n")
        return None
    finally:
        cursor.close()

def menu_opcoes(pergunta: str, opcoes_texto: list[str], opcoes_valor: list[str]) -> str:
    """
    Exibe um menu numerado, solicita a escolha do usuário e retorna o valor correspondente.
    Em caso de escolha inválida, exibe uma mensagem de erro, pausa a execução
    ('Pressione ENTER para continuar...') e limpa a tela (limpa_tela()).
    Continua em loop até que uma escolha válida seja feita.

    Args:
        pergunta: A mensagem a ser exibida antes das opções.
        opcoes_texto: Uma lista de strings com os textos a serem exibidos no menu.
        opcoes_valor: Uma lista de strings com os valores de retorno correspondentes a cada opção.

    Returns:
        str: O valor de retorno da opção escolhida.
    """
    while True:
        print(pergunta)
        for i, texto in enumerate(opcoes_texto, 1):
            print(f"{i} - {texto}")
        escolha = input("\nEscolha: ").strip()
        if escolha.isdigit() and 1 <= int(escolha) <= len(opcoes_valor):
            return opcoes_valor[int(escolha)-1]
        else:
            print(f"\n {margem} Escolha inválida!\n")
            input("Pressione ENTER para continuar...")
            limpa_tela()

def menu_opcoes2(pergunta: str, opcoes_texto: list[str], opcoes_valor: list[str]) -> str:
    """
    Exibe um menu numerado, solicita a escolha do usuário e retorna o valor correspondente.
    Em caso de escolha inválida, exibe a mensagem de erro e retorna ao topo do loop
    imediatamente, sem pausar ou limpar a tela.

    Args:
        pergunta: A mensagem a ser exibida antes das opções.
        opcoes_texto: Uma lista de strings com os textos a serem exibidos no menu.
        opcoes_valor: Uma lista de strings com os valores de retorno correspondentes.

    Returns:
        str: O valor de retorno da opção escolhida.
    """
    while True:
        print(pergunta)
        for i, texto in enumerate(opcoes_texto, 1):
            print(f"{i} - {texto}")
        escolha = input("\nEscolha: ").strip()
        if escolha.isdigit() and 1 <= int(escolha) <= len(opcoes_valor):
            return opcoes_valor[int(escolha)-1]
        else:
            print(f"\n {margem} Escolha inválida!\n")

# ====== CONEXÃO ======

def conectarBD() -> oracledb.Connection | None:
    """
    Estabelece uma conexão com o banco de dados Oracle.
    Usa as credenciais e a DSN (Data Source Name) fixas para tentar a conexão.
    Exibe uma mensagem de sucesso ou uma mensagem de erro em caso de falha.

    Returns:
        oracledb.Connection: O objeto de conexão ativo, se a conexão for bem-sucedida.
        None: Se ocorrer qualquer erro durante a tentativa de conexão.
    """
    try:
        conn = oracledb.connect(user="RM565698", password="200591", dsn="oracle.fiap.com.br:1521/ORCL")
    except Exception as e:
        print(f"\n {margem} Erro ao conectar no banco de dados: {e} \n")
        return None
    else:
        print(f"\n {margem} Conexão realizada!\n")
        return conn

# ====== API EXTERNA ======

def endereco_cep(cep: str) -> dict | None:
    """
    Busca dados de endereço (logradouro, bairro, cidade, estado) usando a API ViaCEP.
    Limpa e valida o CEP fornecido (deve ter 8 dígitos). Faz uma requisição HTTP
    para a API e trata a resposta JSON, incluindo casos de CEP não encontrado.

    Args:
        cep: A string contendo o CEP a ser consultado.

    Returns:
        dict: Um dicionário com as chaves "logradouro", "bairro", "cidade" e "estado", se o CEP for encontrado.
        None: Se o CEP for inválido, não for encontrado, ou se ocorrer um erro na consulta.
    """
    try:
        cep = cep.strip().replace("-", "").replace(".", "")
        if len(cep) != 8 or not cep.isdigit():
            print(f"\n {margem} CEP inválido. Deve conter 8 números.\n")
            return None
        url = f"https://viacep.com.br/ws/{cep}/json/"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            dados = response.json()
            if "erro" not in dados:
                return {"logradouro": dados["logradouro"], "bairro": dados["bairro"], "cidade": dados["localidade"], "estado": dados["uf"]}
        print(f"\n {margem} CEP não encontrado.\n")
    except Exception as e:
        print(f"\n {margem} Erro ao consultar o CEP: {e}\n")
    return None

# ====== CRUD CADASTRO ======

def cadastrar_colaborador(conn: oracledb.Connection) -> None:
    """
    Solicita, valida e insere os dados de um novo colaborador na tabela T_MNDSH_COLABORADOR usando INSERT e trata exceções de conexão.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.

    Returns:
        None: A função realiza a ação de inserção.

    Dependências:
        - Funções: limpa_tela, perguntar_continuar, perguntar_continuar2, validar_cpf,
                   cpf_unico, validar_data, data_datetime, menu_opcoes2, endereco_cep,
                   parse_salario.
        - Variáveis: margem.
    """
    while True:
        limpa_tela()
        print("===== CADASTRAR COLABORADOR =====\n")
        try:
            cursor = conn.cursor()
            # CPF
            while True:
                cpf = input("\nCPF (11 dígitos, somente números): ").strip()
                if not validar_cpf(cpf):
                    print(f"\n {margem} CPF inválido! Digite 11 números.\n")
                    if not perguntar_continuar("informar outro CPF"):
                        return
                    continue
                if not cpf_unico(conn, cpf):
                    print(f"\n {margem} CPF já cadastrado! Não é possível cadastrar novamente.\n")
                    if not perguntar_continuar("informar outro CPF"):
                        return
                    continue
                break
            # NOME
            while True:
                nome = input("\nNome: ").strip()
                if not nome:
                    print(f"\n {margem} Nome é obrigatório.\n")
                    if not perguntar_continuar("informar outro nome"):
                        return
                    continue
                if len(nome) < 3:
                    print(f"\n {margem} O nome deve ter pelo menos 3 caracteres.\n")
                    if not perguntar_continuar("informar outro nome"):
                        return
                    continue
                break
            # DATA DE NASCIMENTO
            while True:
                data_nasc = input("\nData de nascimento (DD/MM/AAAA): ").strip()
                if not validar_data(data_nasc):
                    print(f"\n {margem} Data inválida!\n")
                    if not perguntar_continuar("informar outra data de nascimento"):
                        return
                    continue
                data_nasc_dt = data_datetime(data_nasc)
                idade = (datetime.now() - data_nasc_dt).days // 365
                if idade < 16:
                    print(f"\n {margem} O colaborador deve ter pelo menos 16 anos!\n")
                    if not perguntar_continuar("informar outra data de nascimento"):
                        return
                    continue              
                break
            # SEXO
            sexo = menu_opcoes2("\nSexo:", ["Masculino", "Feminino"], ["M", "F"])
            # ENDEREÇO
            while True:
                cep = input("\nCEP: ").strip()
                endereco = endereco_cep(cep)
                if endereco:
                    print(f"\n {margem} Endereço encontrado:\n")
                    print(f"Logradouro: {endereco['logradouro']}")
                    print(f"Bairro: {endereco['bairro']}")
                    print(f"Cidade: {endereco['cidade']}")
                    print(f"Estado: {endereco['estado']}")
                    break
                else:
                    print(f"\n {margem} CEP inválido ou não encontrado.\n")
                    if not perguntar_continuar("informar outro CEP"):
                        return
                    continue
                break
            # NÚMERO DA RESIDENCIA
            while True:
                numero = input("\nNúmero do endereço: ").strip()
                if not numero:
                    print(f"\n {margem} Número é obrigatório.\n")
                    if not perguntar_continuar("informar outro número"):
                        return
                    continue
                if not numero.isdigit():
                    print(f"\n {margem} Número inválido! Digite apenas números.\n")
                    if not perguntar_continuar("informar outro número"):
                        return
                    continue
                break
            # SALARIO
            while True:
                entrada = input("\nSalário: ").strip()
                salario = parse_salario(entrada)
                if salario is None:
                    print(f"\n {margem} Valor inválido para salário!\n")
                    if not perguntar_continuar("informar outro salário"):
                        return
                    continue
                break
            # CARGO
            while True:
                cargo = input("\nCargo: ").strip()
                if not cargo:
                    print(f"\n {margem} Cargo é obrigatório.\n")
                    if not perguntar_continuar("informar outro cargo"):
                        return
                    continue
                if len(cargo) < 3:
                    print(f"\n {margem} O cargo deve ter pelo menos 3 caracteres.\n")
                    if not perguntar_continuar("informar outro cargo"):
                        return
                    continue
                break
            # DATA DE ADMISSÃO 
            while True:
                data_admissao = input("\nData de admissão (DD/MM/AAAA): ").strip()
                if not validar_data(data_admissao):
                    print(f"\n {margem} Data de admissão inválida!\n")
                    if not perguntar_continuar("informar outra data"):
                        return
                    continue
                data_admissao_dt = data_datetime(data_admissao)
                if data_admissao_dt < data_nasc_dt + timedelta(days=16*365):
                    print(f"\n {margem} A data de admissão não pode ser antes de o colaborador completar 16 anos!\n")
                    if not perguntar_continuar("informar outra data"):
                        return
                    continue
                if data_admissao_dt > datetime.now():
                    print(f"\n {margem} A data de admissão não pode ser futura!\n")
                    if not perguntar_continuar("informar outra data"):
                        return
                    continue
                break
            # DATA DE DEMISSÃO - OPCIONAL 
            while True:
                data_demissao = input("\nData de demissão (opcional, DD/MM/AAAA, ou pressione ENTER para continuar): ").strip()
                if not data_demissao:
                    status = "Ativo"
                    data_demissao_dt = None
                    break
                if not validar_data(data_demissao):
                    print(f"\n {margem} Data de demissão inválida!\n")
                    if not perguntar_continuar("informar outra data"):
                        return
                    continue
                data_demissao_dt = data_datetime(data_demissao)
                if data_demissao_dt < data_admissao_dt:
                    print(f"\n {margem} A data de demissão não pode ser antes da admissão!\n")
                    if not perguntar_continuar("informar outra data"):
                        return
                    continue
                if data_demissao_dt > datetime.now():
                    print(f"\n {margem} A data de demissão não pode ser futura!\n")
                    if not perguntar_continuar("informar outra data"):
                        return
                    continue
                #STATUS
                status = "Inativo"
                break
            #DATA DE CRIAÇÃO E ÚLTIMA MODIFICAÇÃO
            dt_criacao = datetime.now()
            dt_ultima_modificacao = datetime.now()
            cursor.execute("""
                INSERT INTO T_MNDSH_COLABORADOR (nr_cpf, nm_colaborador, dt_nascimento, ds_sexo, cep, ds_logradouro, nr_endereco, ds_bairro, ds_cidade, 
                           ds_estado, vl_salario, ds_cargo, dt_admissao, dt_demissao, ds_status, dt_criacao, dt_ultima_modificacao)
                            VALUES (:cpf, :nome, TO_DATE(:nasc, 'DD/MM/YYYY'), :sexo, :cep, :logradouro,:numero, :bairro, :cidade, :estado, :salario, :cargo,
                     TO_DATE(:admissao, 'DD/MM/YYYY'), TO_DATE(:demissao, 'DD/MM/YYYY'),:status, :criacao,:modificacao)
            """, 
            {"cpf": cpf, "nome": nome, "nasc": data_nasc, "sexo": sexo, "cep": cep, "logradouro": endereco['logradouro'],"numero": numero, "bairro": endereco['bairro'], 
             "cidade": endereco['cidade'], "estado": endereco['estado'], "salario": salario, "cargo": cargo, "admissao": data_admissao, 
             "demissao": data_demissao if data_demissao else None, "status": status, "criacao": dt_criacao, "modificacao": dt_ultima_modificacao})
            conn.commit()
            print(f"\n {margem} Colaborador cadastrado com sucesso!\n")
        except Exception as e:
            print(f"\n {margem} Erro ao cadastrar colaborador: {e}\n")
            conn.rollback()
        finally:
            cursor.close()
        if not perguntar_continuar2("cadastrar outro colaborador"):
            break

def listar_colaboradores(conn: oracledb.Connection) -> None:
    """
    Apresenta opções para buscar e listar dados de colaboradores do banco.

    Oferece modos de listagem por ID/CPF, Todos e Pesquisa Genérica.
    Os resultados são convertidos em um DataFrame e exibidos de forma formatada imprimir_tabela(),
    com opção de exportação gerar_dataframe().

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.

    Returns:
        None: A função gerencia a exibição e exportação.

    Dependências:
        - Funções: limpa_tela, menu_opcoes, buscar_colaborador, perguntar_continuar,
                   perguntar_continuar2, imprimir_tabela, gerar_dataframe.
        - Variáveis: margem (para formatação de saída).
    """
    while True:
        cursor = conn.cursor()
        try:
            limpa_tela()
            opcao = menu_opcoes("===== LISTAR COLABORADORES =====\n \nEscolha a forma de pesquisa:", ["ID ou CPF", "Todos", "Pesquisa genérica", "Voltar"], ["ID ou CPF", "TODOS", "GENERICA", "VOLTAR"])
            dados = []
            colunas = ['Id','CPF', 'Nome', 'Data de nascimento', 'Sexo', 'CEP', 'Logradouro', 'Número', 'Bairro', 'Cidade', 'Estado', 
                       'Salário', 'Cargo', 'Data de admissão', 'Data de demissão','Status', 'Data Criação', 'Data última modificação']

            if opcao == "ID ou CPF":
                limpa_tela()
                colaborador = buscar_colaborador(conn,  titulo_menu="LISTAR COLABORADORES")
                if colaborador:
                    dados = [list(colaborador.values())]
                else:
                    continue
            elif opcao == "TODOS":
                query = """
                    SELECT id, nr_cpf, nm_colaborador, dt_nascimento, ds_sexo, cep, ds_logradouro, nr_endereco, ds_bairro,
                           ds_cidade, ds_estado, vl_salario, ds_cargo, dt_admissao, dt_demissao, ds_status, dt_criacao, dt_ultima_modificacao
                    FROM T_MNDSH_COLABORADOR
                    ORDER BY id
                """
                cursor.execute(query)
                dados = cursor.fetchall()
            elif opcao == "GENERICA":
                desistencia = False
                while True:
                    limpa_tela()
                    print("===== LISTAR COLABORADORES =====\n")
                    base = input("\nDigite parte do texto para pesquisa: ").strip()
                    if not base:
                        print(f"\n{margem} Digite pelo menos um trecho de texto!\n")
                        if not perguntar_continuar("tentar novamente"):
                            desistencia = True
                            break 
                        continue
                    query = """
                        SELECT id, nr_cpf, nm_colaborador, dt_nascimento, ds_sexo, cep, ds_logradouro, nr_endereco, ds_bairro,
                            ds_cidade, ds_estado, vl_salario, ds_cargo, dt_admissao, dt_demissao, ds_status, dt_criacao, dt_ultima_modificacao
                        FROM T_MNDSH_COLABORADOR
                        WHERE UPPER(nm_colaborador) LIKE UPPER(:base)
                        OR UPPER(nr_cpf) LIKE UPPER(:base)
                        OR TO_CHAR(dt_nascimento, 'DD/MM/YYYY') LIKE :base
                        OR UPPER(ds_sexo) LIKE UPPER(:base)
                        OR cep LIKE :base
                        OR UPPER(ds_logradouro) LIKE UPPER(:base)
                        OR TO_CHAR(nr_endereco) LIKE :base
                        OR UPPER(ds_bairro) LIKE UPPER(:base)
                        OR UPPER(ds_cidade) LIKE UPPER(:base)
                        OR UPPER(ds_estado) LIKE UPPER(:base)
                        OR TO_CHAR(vl_salario, '999G999D99') LIKE :base
                        OR UPPER(ds_cargo) LIKE UPPER(:base)
                        OR UPPER(ds_status) LIKE UPPER(:base)
                        OR TO_CHAR(dt_admissao, 'DD/MM/YYYY') LIKE :base
                        OR TO_CHAR(dt_demissao, 'DD/MM/YYYY') LIKE :base
                        OR TO_CHAR(dt_criacao, 'DD/MM/YYYY HH24:MI') LIKE :base
                        OR TO_CHAR(dt_ultima_modificacao, 'DD/MM/YYYY HH24:MI') LIKE :base
                        ORDER BY nm_colaborador
                    """
                    cursor.execute(query, {"base": f"%{base}%"})
                    dados = cursor.fetchall()
                    break 
                if desistencia:
                    continue
            elif opcao == "VOLTAR":
                print(f"\n{margem} Voltando...\n")
                input("Pressione ENTER para continuar...")
                limpa_tela()
                break
            if dados:
                limpa_tela()
                df = pd.DataFrame(dados, columns=colunas)
                grupos_personalizados = [
                    ("DADOS PESSOAIS", ['Id', 'CPF', 'Nome', 'Data de nascimento', 'Sexo']),
                    ("ENDEREÇO COMPLETO", ['CEP', 'Logradouro', 'Número', 'Bairro', 'Cidade', 'Estado']),
                    ("VÍNCULO EMPREGATÍCIO", ['Salário', 'Cargo', 'Data de admissão', 'Data de demissão', 'Status', 'Data Criação', 'Data última modificação']),
                ]
                imprimir_tabela(df, 
                    titulo="LISTA DE COLABORADORES", 
                    colunas_datas=['Data de nascimento', 'Data de admissão', 'Data de demissão'], 
                    colunas_datetime=['Data Criação', 'Data última modificação'], 
                    colunas_moeda=['Salário'],
                    colunas_exibir=grupos_personalizados)
                gerar_dataframe(df)
        except Exception as e:
            print(f"\n{margem} Erro ao listar colaboradores: {e}\n")
        finally:
            cursor.close()
        if not perguntar_continuar2("realizar outra pesquisa/listagem"):
            break

def atualizar_colaborador(conn: oracledb.Connection) -> None:
    """
    Permite ao usuário atualizar um campo específico de um colaborador no banco.

    Primeiro, busca o colaborador buscar_colaborador(). Em seguida, exibe os dados
    atuais e apresenta um menu para escolher o campo a ser alterado. Cada campo possui
    sua própria lógica de validação. Alterações no CEP, Data de Demissão e Status
    são tratadas separadamente com lógica SQL e atualização do dicionário do colaborador.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.

    Returns:
        None: A função realiza a ação de atualização.

    Dependências:
        - Funções: limpa_tela, buscar_colaborador, imprimir_tabela, menu_opcoes,
                   menu_opcoes2, perguntar_continuar, perguntar_continuar2, validar_cpf,
                   cpf_unico, validar_data, data_datetime, endereco_cep, parse_salario.
        - Variáveis: margem, mapeamento_colunas.
    """
    while True:
        limpa_tela()
        try:
            colaborador_dicionario = buscar_colaborador(conn, titulo_menu="ATUALIZAR COLABORADOR")
            if not colaborador_dicionario:
                return
            campos = {"Nome": "nm_colaborador", "CPF": "nr_cpf", "Data de nascimento": "dt_nascimento", "Sexo": "ds_sexo", "CEP": "cep", "Número": "nr_endereco", 
                      "Salário": "vl_salario", "Cargo": "ds_cargo", "Data de admissão": "dt_admissao", "Data de demissão": "dt_demissao"}         
            while True:
                limpa_tela()
                grupos_personalizados = [
                    ("DADOS PESSOAIS", ['Id', 'CPF', 'Nome', 'Data de nascimento', 'Sexo']),
                    ("ENDEREÇO COMPLETO", ['CEP', 'Logradouro', 'Número', 'Bairro', 'Cidade', 'Estado']),
                    ("VÍNCULO EMPREGATÍCIO", ['Salário', 'Cargo', 'Data de admissão', 'Data de demissão', 'Status', 'Data Criação', 'Data última modificação']),
                ]
                df = pd.DataFrame([colaborador_dicionario.copy()]) 
                df.rename(columns=mapeamento_colunas, inplace=True)
                imprimir_tabela(df, 
                                titulo="DADOS ATUAIS DO COLABORADOR", 
                                colunas_datas=['Data de nascimento', 'Data de admissão', 'Data de demissão'],
                                colunas_datetime=['Data Criação', 'Data última modificação'], 
                                colunas_moeda=['Salário'], 
                                colunas_exibir=grupos_personalizados)

                escolha = menu_opcoes("\nQual campo deseja alterar?", list(campos.keys()) + ["Finalizar"], list(campos.keys()) + ["Finalizar"])

                if escolha == "Finalizar":
                    print(f"\n {margem} Finalizando...\n")
                    input("\nPressione ENTER para continuar...")
                    limpa_tela()
                    break

                campo_sql = campos[escolha]
                novo_valor = None 

                while True: 
                    # SEXO 
                    if escolha == "Sexo":
                        novo_valor = menu_opcoes2("\nInforme o sexo:", ["Masculino", "Feminino"], ["M", "F"])
                        break

                    entrada = input(f"\nNovo valor para {escolha}: ").strip()
                    
                    if not entrada and escolha != "Data de demissão":
                         print(f"\n {margem} Valor inválido. Não pode ficar vazio.\n")
                         if not perguntar_continuar("tentar novamente"):
                            break 
                         continue   
                    # NOME
                    if escolha == "Nome":
                        if len(entrada) < 3:
                            print(f"\n {margem} Nome deve possuir pelo menos 3 caracteres.\n")
                            if not perguntar_continuar("tentar novamente"):
                                break 
                            continue
                        novo_valor = entrada                      
                    # CPF
                    elif escolha == "CPF":
                        if not validar_cpf(entrada):
                            print(f"\n {margem} CPF inválido! Digite 11 números.\n")
                            if not perguntar_continuar("informar outro CPF"):
                                break 
                            continue
                        if not cpf_unico(conn, entrada) and entrada != colaborador_dicionario["nr_cpf"]:
                            print(f"\n {margem} CPF já cadastrado! Não é possível cadastrar novamente.\n")
                            if not perguntar_continuar("informar outro CPF"):
                                break 
                            continue
                        novo_valor = entrada
                    # DATA DE NASCIMENTO 
                    elif escolha == "Data de nascimento":
                        if not validar_data(entrada):
                            print(f"\n {margem} Data inválida!\n")
                            if not perguntar_continuar("informar outra data de nascimento"):
                                break 
                            continue                           
                        data_nasc_dt = data_datetime(entrada)
                        idade = (datetime.now() - data_nasc_dt).days // 365
                        if idade < 16:
                            print(f"\n {margem} O colaborador deve ter pelo menos 16 anos!\n")
                            if not perguntar_continuar("informar outra data de nascimento"):
                                break 
                            continue
                        # DATA DE ADMISSÃO 
                        data_admissao_valor_atual = colaborador_dicionario["dt_admissao"] 
                        
                        if data_admissao_valor_atual and pd.notna(data_admissao_valor_atual): 
                            if isinstance(data_admissao_valor_atual, datetime):
                                data_admissao_str_atual = data_admissao_valor_atual.strftime('%d/%m/%Y')
                            else:
                                data_admissao_str_atual = str(data_admissao_valor_atual)
                            
                            data_admissao_dt_atual = data_datetime(data_admissao_str_atual)

                            idade_min_admissao = data_nasc_dt + timedelta(days=16*365)
                            
                            if data_admissao_dt_atual < idade_min_admissao:
                                print(f"\n {margem} Esta nova data de nascimento torna a data de admissão ({colaborador_dicionario['dt_admissao']}) inválida.\n")
                                print(f"\n {margem} Altere a data de admissão ou desista desta alteração.")
                                if not perguntar_continuar("alterar outro campo"):
                                    break 
                                continue

                        novo_valor = entrada 
                    # CEP 
                    elif escolha == "CEP":                        
                        endereco = endereco_cep(entrada)
                        if not endereco:
                            print(f"\n {margem} CEP inválido ou não encontrado.\n")
                            if not perguntar_continuar("informar outro CEP"):
                                break 
                            continue

                        print(f"\n {margem} Endereço encontrado:")
                        print(f" {margem} Logradouro: {endereco['logradouro']}")
                        print(f" {margem} Bairro: {endereco['bairro']}")
                        print(f" {margem} Cidade: {endereco['cidade']}")
                        print(f" {margem} Estado: {endereco['estado']}")
                        
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE T_MNDSH_COLABORADOR
                            SET cep = :cep, ds_logradouro = :logradouro, ds_bairro = :bairro, ds_cidade = :cidade,
                                ds_estado = :estado, dt_ultima_modificacao = SYSDATE
                            WHERE id = :id
                        """, {"cep": entrada, "logradouro": endereco["logradouro"], "bairro": endereco["bairro"],
                            "cidade": endereco["cidade"], "estado": endereco["estado"], "id": colaborador_dicionario["id"]})
                        conn.commit()
                        cursor.close()

                        colaborador_dicionario.update({"cep": entrada, "ds_logradouro": endereco["logradouro"],
                            "ds_bairro": endereco["bairro"],"ds_cidade": endereco["cidade"], "ds_estado": endereco["estado"]})
                            
                        print(f"\n {margem} Endereço atualizado com sucesso!\n")
                        input("Pressione ENTER...")
                        
                        novo_valor = "CEP_ATUALIZADO" 
                        break 
                    # NÚMERO DA RESIDÊNCIA
                    elif escolha == "Número":
                        if not entrada.isdigit():
                            print(f"\n {margem} Número inválido! Digite apenas números.\n")
                            if not perguntar_continuar("informar outro número"):
                                break 
                            continue
                        novo_valor = int(entrada)
                    # SALÁRIO
                    elif escolha == "Salário":
                        salario = parse_salario(entrada)
                        if salario is None:
                            print(f"\n {margem} Valor inválido para salário!\n")
                            if not perguntar_continuar("informar outro salário"):
                                break 
                            continue
                        novo_valor = salario                        
                    # CARGO
                    elif escolha == "Cargo":
                        if len(entrada) < 3:
                            print(f"\n {margem} O cargo deve ter pelo menos 3 caracteres.\n")
                            if not perguntar_continuar("informar outro cargo"):
                                break 
                            continue
                        novo_valor = entrada
                    # DATA DE ADMISSÃO
                    elif escolha == "Data de admissão":
                        data_nasc_valor_atual = colaborador_dicionario["dt_nascimento"]
                        if isinstance(data_nasc_valor_atual, datetime):
                            data_nasc_str_atual = data_nasc_valor_atual.strftime('%d/%m/%Y')
                        else:
                            data_nasc_str_atual = str(data_nasc_valor_atual)
                        data_nasc_dt_atual = data_datetime(data_nasc_str_atual)
                        if not validar_data(entrada):
                            print(f"\n {margem} Data de admissão inválida!\n")
                            if not perguntar_continuar("informar outra data"):
                                break 
                            continue
                            
                        data_admissao_dt = data_datetime(entrada)
                        if data_admissao_dt < data_nasc_dt_atual + timedelta(days=16*365):
                            print(f"\n {margem} A data de admissão não pode ser antes de o colaborador completar 16 anos!\n")
                            if not perguntar_continuar("informar outra data"):
                                break 
                            continue
                        if data_admissao_dt > datetime.now():
                            print(f"\n {margem} A data de admissão não pode ser futura!\n")
                            if not perguntar_continuar("informar outra data"):
                                break 
                            continue
                        novo_valor = entrada 
                        break 
                    # DATA DE DEMISSÃO
                    elif escolha == "Data de demissão":
                        data_admissao_valor_atual = colaborador_dicionario["dt_admissao"]
                        data_admissao_dt_atual = None
                        if pd.notna(data_admissao_valor_atual):
                            if isinstance(data_admissao_valor_atual, datetime):
                                data_admissao_str_atual = data_admissao_valor_atual.strftime('%d/%m/%Y')
                            else:
                                data_admissao_str_atual = str(data_admissao_valor_atual)
                            try:
                                data_admissao_dt_atual = data_datetime(data_admissao_str_atual)
                            except:
                                pass 
                        if not entrada:
                            cursor = conn.cursor()
                            cursor.execute("""
                                UPDATE T_MNDSH_COLABORADOR
                                SET dt_demissao = NULL, ds_status = 'Ativo', dt_ultima_modificacao = SYSDATE
                                WHERE id = :id
                            """, {"id": colaborador_dicionario["id"]})
                            conn.commit()
                            cursor.close()

                            colaborador_dicionario["dt_demissao"] = None
                            colaborador_dicionario["ds_status"] = "Ativo"
                            print(f"\n {margem} Data de demissão removida e status atualizado para Ativo com sucesso!\n")
                            input("Pressione ENTER...")
                            novo_valor = "DEMISSAO_ATUALIZADA" 
                            break 
                        else:
                            if not validar_data(entrada):
                                print(f"\n {margem} Data de demissão inválida!\n")
                                if not perguntar_continuar("informar outra data"):
                                    break 
                                continue
                            data_demissao_dt = data_datetime(entrada)
                            if data_admissao_dt_atual and data_demissao_dt < data_admissao_dt_atual:
                                print(f"\n {margem} A data de demissão não pode ser antes da admissão!\n")
                                if not perguntar_continuar("informar outra data"):
                                    break 
                                continue
                            if data_demissao_dt > datetime.now():
                                print(f"\n {margem} A data de demissão não pode ser futura!\n")
                                if not perguntar_continuar("informar outra data"):
                                    break 
                                continue
                                
                            novo_valor = entrada 
                            status = "Inativo"
                            
                            cursor = conn.cursor()
                            cursor.execute("""
                                UPDATE T_MNDSH_COLABORADOR
                                SET ds_status = :status, dt_ultima_modificacao = SYSDATE
                                WHERE id = :id
                            """, {"status": status, "id": colaborador_dicionario["id"]})
                            conn.commit()
                            cursor.close()
                            colaborador_dicionario["ds_status"] = status
                            break 
                    break 

                if novo_valor is None or novo_valor == "CEP_ATUALIZADO" or novo_valor == "DEMISSAO_ATUALIZADA":
                    continue

                cursor = conn.cursor()
                if escolha in ["Data de nascimento", "Data de admissão", "Data de demissão"]:
                    sql = f"""
                        UPDATE T_MNDSH_COLABORADOR
                        SET {campo_sql} = TO_DATE(:valor, 'DD/MM/YYYY'),
                            dt_ultima_modificacao = SYSDATE
                        WHERE id = :id
                    """
                else:
                    sql = f"""
                        UPDATE T_MNDSH_COLABORADOR
                        SET {campo_sql} = :valor,
                            dt_ultima_modificacao = SYSDATE
                        WHERE id = :id
                    """
                cursor.execute(sql, {"valor": novo_valor, "id": colaborador_dicionario["id"]})
                conn.commit()
                cursor.close()
                colaborador_dicionario[campo_sql] = novo_valor
                print(f"\n {margem} {escolha} atualizado com sucesso!\n")
                input("Pressione ENTER...")   
        except Exception as e:
            print(f"\n{margem} Erro ao atualizar colaborador: {e}\n")
            conn.rollback()
        if not perguntar_continuar2("atualizar outro colaborador"):
            break

def excluir_colaborador(conn: oracledb.Connection) -> None:
    """
    Busca um colaborador por ID/CPF e, após confirmação, o exclui (DELETE) do banco de dados.
    Exibe os dados completos do colaborador antes de solicitar a confirmação final.
    Em caso de sucesso, realiza o commit.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.

    Returns:
        None: A função realiza a ação de exclusão.

    Dependências:
        - Funções: limpa_tela, buscar_colaborador, imprimir_tabela, menu_opcoes2,
                   perguntar_continuar2.
        - Variáveis: margem, mapeamento_colunas.
    """
    while True:
        limpa_tela()
        cursor = conn.cursor()
        try:
            colaborador = buscar_colaborador(conn, titulo_menu="EXCLUIR COLABORADOR")
            if not colaborador:  
                break
            limpa_tela()
            grupos_personalizados = [
                    ("DADOS PESSOAIS", ['Id', 'CPF', 'Nome', 'Data de nascimento', 'Sexo']),
                    ("ENDEREÇO COMPLETO", ['CEP', 'Logradouro', 'Número', 'Bairro', 'Cidade', 'Estado']),
                    ("VÍNCULO EMPREGATÍCIO", ['Salário', 'Cargo', 'Data de admissão', 'Data de demissão', 'Status', 'Data Criação', 'Data última modificação']),
                ]
            df = pd.DataFrame([colaborador.copy()]) 
            df.rename(columns=mapeamento_colunas, inplace=True) 
            imprimir_tabela(df, 
                            titulo=f"DADOS DO COLABORADOR: {colaborador.get('nm_colaborador','')}", 
                            colunas_datas=['Data de nascimento', 'Data de admissão', 'Data de demissão'], 
                            colunas_datetime=['Data Criação', 'Data última modificação'], 
                            colunas_moeda=['Salário'], 
                            colunas_exibir=grupos_personalizados)
            confirmacao = menu_opcoes2(f"\nTem certeza que deseja excluir o colaborador {colaborador.get('nm_colaborador','')} - CPF: {colaborador.get('nr_cpf','')} (ID: {colaborador['id']})?", ["Sim", "Não"], ["S", "N"])
            if confirmacao != "S":
                print(f"\n {margem} Exclusão cancelada.\n")
                input("\nPressione ENTER para continuar...")
                limpa_tela()
            else:
                cursor.execute("DELETE FROM T_MNDSH_COLABORADOR WHERE id = :id", {"id": colaborador["id"]})
                conn.commit()
                print(f"\n {margem} Colaborador excluído com sucesso!\n")
                input("\nPressione ENTER para continuar...")
                limpa_tela()
        except Exception as e:
            print(f"\n {margem} Erro ao excluir colaborador:", e, "\n")
            conn.rollback()
        finally:
            cursor.close()
        if not perguntar_continuar2("excluir outro colaborador"):
            break

# ====== CRUD tarefas ======

# ADMINISTRADOR 

def adicionar_tarefa_admin(conn: oracledb.Connection) -> None:
    """
    Permite ao administrador selecionar um colaborador e atribuir uma nova tarefa.

    A função solicita o ID/CPF do colaborador buscar_colaborador(), o título, a descrição, a prioridade (via menu) e o prazo.
    O prazo é validado para garantir que seja uma data futura. A tarefa é
    inserida na tabela T_MNDSH_TAREFA com o status inicial 'pendente'.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.

    Returns:
        None: A função realiza a ação de inserção.

    Dependências:
        - Funções: limpa_tela, buscar_colaborador, perguntar_continuar, menu_opcoes2.
        - Variáveis: margem.
    """
    while True: 
        limpa_tela()
        cursor = conn.cursor()
        try:
            colaborador = buscar_colaborador(conn, titulo_menu="ADICIONAR NOVA TAREFA")
            if not colaborador:
                return
            id_colaborador = colaborador["id"]
            cpf = colaborador["nr_cpf"]
            nome = colaborador["nm_colaborador"]
            status = colaborador["ds_status"]
            print(f"\n {margem} Colaborador selecionado: {nome} (CPF: {cpf}) Status: {status}\n")
            while True:
                voltar_para_colaborador = False 
                while True:
                    titulo = input("\nTítulo: ").strip()
                    if len(titulo) < 5:
                        print(f"\n {margem} O título deve conter ao menos 5 caracteres.\n")
                        if perguntar_continuar("tentar novamente"):
                            continue
                        else:
                            voltar_para_colaborador = True
                            break 
                    break
                if voltar_para_colaborador:
                    break 

                descricao = input("\nDescrição (opcional): ").strip() or None

                prioridade = menu_opcoes2("\nPrioridade da tarefa:", ["Baixa", "Média", "Alta"], ["baixa", "média", "alta"])

                while True:
                    data_prazo = input("\nPrazo (DD/MM/AAAA): ").strip()
                    voltar_para_colaborador = False 
                    try:
                        dt_prazo = datetime.strptime(data_prazo, "%d/%m/%Y")
                        if dt_prazo < datetime.today():
                            print(f"\n {margem} A data deve ser hoje ou futura.\n")
                            if perguntar_continuar("tentar novamente"):
                                continue
                            else:
                                voltar_para_colaborador = True
                                break
                        break
                    except ValueError:
                        print(f"\n {margem} Formato inválido! Use DD/MM/AAAA.\n")
                        if perguntar_continuar("tentar novamente"):
                            continue
                        else:
                            voltar_para_colaborador = True
                            break

                if voltar_para_colaborador:
                    break

                cursor.execute("""
                    INSERT INTO T_MNDSH_TAREFA(id_colaborador, nr_cpf, ds_titulo, ds_descricao,ds_prioridade, dt_prazo, ds_status, dt_criacao, dt_modificacao)
                    VALUES (:id, :cpf, :titulo, :descricao, :prioridade, TO_DATE(:prazo, 'DD/MM/YYYY'), 'pendente', SYSDATE, SYSDATE)
                """, {"id": id_colaborador, "cpf": cpf, "titulo": titulo, "descricao": descricao, "prioridade": prioridade, "prazo": dt_prazo.strftime("%d/%m/%Y")})
                conn.commit()
                print(f"\n {margem} Tarefa adicionada com sucesso!\n")
                input("Pressione ENTER...")
                limpa_tela()

                if perguntar_continuar("adicionar outra tarefa para este colaborador"):
                    limpa_tela()
                    print("===== ADICIONAR NOVA TAREFA =====\n")
                    print(f"{margem} Colaborador selecionado: {nome} (CPF: {cpf}) Status: {status}\n")
                    continue
                else:
                    break         
        except Exception as e:
            conn.rollback()
            print(f"\nErro ao adicionar tarefa: {e}\n")
            input("ENTER...")
        finally:
            cursor.close()
        if not perguntar_continuar2("adicionar tarefa para outro colaborador"):
                break

def listar_tarefas_admin(conn: oracledb.Connection) -> None:
    """
    Permite ao administrador listar tarefas com opções de filtro.
    Oferece filtros por:
    1. Colaborador (todos ou um específico, via buscar_colaborador().
    2. Status (todas, pendentes, em andamento, concluídas).
    Os resultados são consultados, formatados em um DataFrame (com colunas adaptadas)
    e exibidos via imprimir_tabela(), com opção de exportação gerar_dataframe().

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.

    Returns:
        None: A função gerencia a exibição e exportação.

    Dependências:
        - Funções: limpa_tela, menu_opcoes, buscar_colaborador, perguntar_continuar2,
                   imprimir_tabela, gerar_dataframe.
        - Variáveis: margem.
        - Módulo: pandas (pd)
    """
    while True:
        limpa_tela()
        cursor = conn.cursor()
        try:
            filtro = menu_opcoes("===== LISTAR TAREFAS =====\n \nDeseja listar:\n", ["Todos os colaboradores", "Por colaborador", "Voltar"], ["todos", "colaborador", "voltar"])
            if filtro == "voltar":
                print(f"\n {margem} Voltando...!\n")
                input("\nPressione ENTER para continuar...")
                limpa_tela()
                break
            cpf = None
            nome = None
            params = {} 
            colunas = []

            filtro_status_pendente = menu_opcoes("\nDeseja filtrar por status:\n", 
                                        ["Todas", "Pendentes","Em Andamento", "Concluídas"], 
                                        ["todas", "pendentes","em andamento", "concluidas"])
            filtro_status = ""
            if filtro_status_pendente == "pendentes":
                filtro_status = " AND t.ds_status = 'pendente'"
            elif filtro_status_pendente == "em andamento":
                filtro_status = " AND t.ds_status = 'em andamento'"
            elif filtro_status_pendente == "concluidas":
                filtro_status = " AND t.ds_status = 'concluída'"

            if filtro == "todos":
                query = f"""
                    SELECT t.id_tarefa, t.nr_cpf, c.nm_colaborador, t.ds_titulo, t.ds_descricao, t.ds_status, t.ds_prioridade, t.dt_prazo, t.dt_criacao, t.dt_modificacao
                    FROM T_MNDSH_TAREFA t
                    JOIN T_MNDSH_COLABORADOR c ON t.nr_cpf = c.nr_cpf
                    WHERE 1=1 {filtro_status}
                    ORDER BY t.dt_prazo
                """
                cursor.execute(query, params)
                tarefas = cursor.fetchall()
                colunas = ["ID", "CPF", "Colaborador", "Título", "Descrição", "Status", "Prioridade", "Prazo", "Data Criação", "Data Última Modificação"]

                titulo = f"LISTA DE TAREFAS — {filtro_status_pendente.upper()}"
            else:
                limpa_tela()
                colaborador = buscar_colaborador(conn, titulo_menu="LISTAR TAREFAS")
                if not colaborador:
                    continue
                cpf = colaborador["nr_cpf"]
                nome = colaborador["nm_colaborador"]
                params["cpf"] = cpf

                query = f"""
                    SELECT id_tarefa, ds_titulo, ds_descricao, ds_status, ds_prioridade, dt_prazo, dt_criacao, dt_modificacao
                    FROM T_MNDSH_TAREFA t
                    WHERE nr_cpf = :cpf {filtro_status}
                    ORDER BY dt_prazo
                """
                cursor.execute(query, params)
                tarefas = cursor.fetchall()
                colunas = ["ID", "Título", "Descrição", "Status", "Prioridade", "Prazo", "Data Criação", "Data Última Modificação"]
                titulo = f"TAREFAS ({filtro_status_pendente.upper()}) — {nome} (CPF: {cpf})"
            if tarefas:
                df = pd.DataFrame(tarefas, columns=colunas)
                limpa_tela()
                imprimir_tabela(df, 
                                titulo=titulo, 
                                tamanhos_wrap={"Título":25,"Descrição":40}, 
                                colunas_datas=["Prazo"], 
                                colunas_datetime=["Data Criação", "Data Última Modificação"])
                gerar_dataframe(df)
            else:
                limpa_tela()
                print(f"===== {titulo} =====")
                print(f"\n {margem} Nenhuma tarefa encontrada.\n")
        except Exception as e:
            print(f"\n {margem} Erro ao listar tarefas: {e}\n")
            input("\nPressione ENTER para continuar...")
        finally:
            cursor.close()
        if not perguntar_continuar2("realizar outra pesquisa/listagem"):
            break

def atualizar_tarefa_admin(conn: oracledb.Connection) -> None:
    """
    Permite ao administrador atualizar qualquer campo de uma tarefa atribuída a um colaborador.

    O processo envolve:
    1. Selecionar o colaborador buscar_colaborador().
    2. Listar todas as tarefas do colaborador.
    3. O administrador escolhe o ID da tarefa a ser alterada.
    4. Apresenta um menu para escolha do campo (Título, Descrição, Status, Prioridade, Prazo).
    5. Cada campo tem validação específica.
    6. Executa o UPDATE no banco e atualiza a visualização.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.

    Returns:
        None: A função realiza a ação de atualização.

    Dependências:
        - Funções: limpa_tela, buscar_colaborador, imprimir_tabela, menu_opcoes,
                   menu_opcoes2, perguntar_continuar, perguntar_continuar2.
        - Variáveis: margem.
        - Módulo: pandas (pd).
    """
    while True:
        cursor = conn.cursor()
        limpa_tela()
        try:
            colaborador = buscar_colaborador(conn, titulo_menu="ATUALIZAR TAREFA")
            if not colaborador:
                return
            cpf = colaborador["nr_cpf"]
            nome = colaborador["nm_colaborador"]
            
            cursor.execute("""
                SELECT id_tarefa, ds_titulo, ds_descricao, ds_status, ds_prioridade, dt_prazo, dt_criacao, dt_modificacao
                FROM T_MNDSH_TAREFA
                WHERE nr_cpf = :cpf
                ORDER BY dt_prazo
            """, {"cpf": cpf})
            tarefas = cursor.fetchall()
            if not tarefas:
                print(f"\n===== TAREFAS DE {nome} (CPF: {cpf}) =====\n")
                print(f"{margem} Nenhuma tarefa registrada.\n")
                input("Pressione ENTER para continuar...")
                continue

            colunas = ["ID", "Título", "Descrição", "Status", "Prioridade", "Prazo", "Data Criação", "Data Última Modificação"]
            df_tarefas = pd.DataFrame(tarefas, columns=colunas)

            while True:
                limpa_tela()
                imprimir_tabela(df_tarefas, 
                                titulo=f"TAREFAS DE {nome} (CPF: {cpf})", 
                                tamanhos_wrap={"Título":25,"Descrição":40}, 
                                colunas_datas=["Prazo"],
                                colunas_datetime=["Data Criação", "Data Última Modificação"])
                try:
                    id_tarefa = int(input("\nID da tarefa que deseja atualizar: "))
                except ValueError:
                    print(f"\n{margem} ID inválido. Digite apenas números.\n")
                    if not perguntar_continuar2("tentar novamente"):
                        break
                    continue
                cursor.execute("""
                    SELECT id_tarefa, ds_titulo, ds_descricao, ds_status, ds_prioridade, dt_prazo
                    FROM T_MNDSH_TAREFA
                    WHERE id_tarefa=:id AND nr_cpf=:cpf
                """, {"id": id_tarefa, "cpf": cpf})
                tarefa = cursor.fetchone()
                if not tarefa:
                    print(f"\n{margem} Tarefa não encontrada.\n")
                    if not perguntar_continuar2("tentar novamente"):
                        break
                    continue

                campos = {"Título": "ds_titulo", "Descrição": "ds_descricao", "Status": "ds_status", "Prioridade": "ds_prioridade", "Prazo": "dt_prazo"}

                editar_tarefa = True
                while editar_tarefa:
                    novo_valor = None 
                    limpa_tela()
                    df_tarefa_atual = pd.DataFrame([tarefa], columns=["ID", "Título", "Descrição", "Status", "Prioridade", "Prazo"])
                    imprimir_tabela(df_tarefa_atual, 
                                    titulo="DADOS ATUAIS DA TAREFA", 
                                    tamanhos_wrap={"Título":25,"Descrição":40}, 
                                    colunas_datas=["Prazo"])

                    escolha = menu_opcoes("\nQual campo deseja alterar?", list(campos.keys()) + ["Finalizar"], list(campos.keys()) + ["Finalizar"])

                    if escolha == "Finalizar":
                        editar_tarefa = False
                        break

                    campo_sql = campos[escolha]

                    if escolha == "Título":
                        while True:
                            entrada = input("\nNovo título: ").strip()
                            if len(entrada) < 5:
                                print(f"\n{margem} O título deve ter pelo menos 5 caracteres.\n")
                                if not perguntar_continuar("tentar novamente"):
                                    break
                                continue
                            novo_valor = entrada
                            break
                    elif escolha == "Descrição":
                        novo_valor = input("\nNova descrição (pode ficar vazia): ").strip()
                    elif escolha == "Prioridade":
                        novo_valor = menu_opcoes2("\nEscolha a prioridade:", ["Baixa", "Média", "Alta"], ["baixa", "média", "alta"])
                    elif escolha == "Status":
                        novo_valor = menu_opcoes2("\nEscolha o status:", ["Pendente", "Em andamento", "Concluída"], ["pendente", "em andamento", "concluída"])
                    elif escolha == "Prazo":
                        while True:
                            entrada = input("\nNovo prazo (DD/MM/AAAA): ").strip()
                            if not entrada:
                                print(f"\n{margem} Valor inválido. Não pode ficar vazio.")
                                if not perguntar_continuar("tentar novamente"):
                                    break
                                continue
                            try:
                                dt_novo = datetime.strptime(entrada, "%d/%m/%Y")
                                if dt_novo < datetime.today():
                                    print(f"\n{margem} A data deve ser hoje ou futura.")
                                    if not perguntar_continuar("tentar novamente"):
                                        break
                                    continue
                                novo_valor = entrada
                                break
                            except ValueError:
                                print(f"\n{margem} Formato inválido! Use DD/MM/AAAA.")
                                if not perguntar_continuar("tentar novamente"):
                                    break
                    if novo_valor is None:
                        continue
                    if escolha == "Prazo":
                        cursor.execute(f"""
                            UPDATE T_MNDSH_TAREFA
                            SET {campo_sql} = TO_DATE(:valor,'DD/MM/YYYY'), dt_modificacao=SYSDATE
                            WHERE id_tarefa=:id
                        """, {"valor": novo_valor, "id": id_tarefa})
                    else:
                        cursor.execute(f"""
                            UPDATE T_MNDSH_TAREFA
                            SET {campo_sql} = :valor, dt_modificacao=SYSDATE
                            WHERE id_tarefa=:id
                        """, {"valor": novo_valor, "id": id_tarefa})
                    conn.commit()
                    print(f"\n{margem} {escolha} atualizado com sucesso!\n")
                    input("Pressione ENTER...")
                    cursor.execute("""
                        SELECT id_tarefa, ds_titulo, ds_descricao, ds_status, ds_prioridade, dt_prazo
                        FROM T_MNDSH_TAREFA
                        WHERE id_tarefa=:id
                    """, {"id": id_tarefa})
                    tarefa = cursor.fetchone()
                if not perguntar_continuar2("atualizar outra tarefa deste colaborador"):
                    break 
        except Exception as e:
            print(f"\n{margem} Erro ao atualizar tarefa: {e}\n")
            conn.rollback()
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
        if not perguntar_continuar2("atualizar tarefa de outro colaborador"):
            break

def excluir_tarefa_admin(conn: oracledb.Connection) -> None:
    """
    Permite ao administrador excluir uma tarefa específica atribuída a um colaborador.
    O administrador seleciona o colaborador buscar_colaborador(), lista suas tarefas
    e escolhe o ID da tarefa a ser excluída. Uma confirmação final é solicitada antes
    da execução do DELETE.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.

    Returns:
        None: A função realiza a ação de exclusão.

    Dependências:
        - Funções: limpa_tela, buscar_colaborador, imprimir_tabela, menu_opcoes2,
                   perguntar_continuar2.
        - Variáveis: margem.
        - Módulo: pandas (pd).
    """
    while True:
        limpa_tela()
        cursor = conn.cursor()
        try:
            colaborador = buscar_colaborador(conn, titulo_menu="EXCLUIR TAREFA")
            if not colaborador:
                return 
            cpf = colaborador["nr_cpf"]
            nome = colaborador["nm_colaborador"]
            while True:  
                cursor.execute("""
                    SELECT id_tarefa, ds_titulo, ds_descricao, ds_status, ds_prioridade, dt_prazo, dt_criacao, dt_modificacao
                    FROM T_MNDSH_TAREFA 
                    WHERE nr_cpf=:cpf 
                    ORDER BY dt_prazo
                """, {"cpf": cpf})
                tarefas = cursor.fetchall()
                if not tarefas:
                    print(f"\n {margem} Nenhuma tarefa encontrada para {nome}.\n")
                    input("\nPressione ENTER para continuar...")
                    break 

                df = pd.DataFrame(tarefas, columns=["ID", "Título", "Descrição", "Status", "Prioridade", "Prazo", "Data Criação", "Data Última Modificação"])
                limpa_tela()
                imprimir_tabela(df, 
                                titulo=f"TAREFAS DE {nome}", 
                                tamanhos_wrap={"Título":25,"Descrição":40}, 
                                colunas_datas=["Prazo"], 
                                colunas_datetime=["Data Criação","Data Última Modificação"])

                tarefa = None
                sair_para_outro_colaborador = False 
                while True:
                    try:
                        id_tarefa = int(input("\nID da tarefa: "))
                    except ValueError:
                        print(f"\n {margem} ID inválido.\n")
                        if not perguntar_continuar2("tentar novamente"):
                            sair_para_outro_colaborador = True
                            break  
                        limpa_tela()
                        imprimir_tabela(df, 
                                        titulo=f"TAREFAS DE {nome}", 
                                        tamanhos_wrap={"Título":25,"Descrição":40}, 
                                        colunas_datas=["Prazo"], 
                                        colunas_datetime=["Data Criação","Data Última Modificação"])
                        continue
                    cursor.execute("""
                        SELECT ds_titulo 
                        FROM T_MNDSH_TAREFA 
                        WHERE id_tarefa=:id AND nr_cpf=:cpf
                    """, {"id": id_tarefa, "cpf": cpf})
                    tarefa = cursor.fetchone()

                    if tarefa:
                        break 
                    print(f"\n {margem} Tarefa não encontrada.\n")
                    if not perguntar_continuar2("tentar novamente"):
                        sair_para_outro_colaborador = True
                        tarefa = None
                        break
                    limpa_tela()
                    imprimir_tabela(df, 
                                    titulo=f"TAREFAS DE {nome}", 
                                    tamanhos_wrap={"Título":25,"Descrição":40}, 
                                    colunas_datas=["Prazo"], 
                                    colunas_datetime=["Data Criação","Data Última Modificação"])

                if sair_para_outro_colaborador:
                    break

                print(f"\nTarefa selecionada: {tarefa[0]}")
                confirmacao = menu_opcoes2("\nConfirma exclusão?", ["Sim", "Não"], ["S", "N"])
                if confirmacao == "S":
                    cursor.execute("""
                        DELETE FROM T_MNDSH_TAREFA 
                        WHERE id_tarefa=:id AND nr_cpf=:cpf
                    """, {"id": id_tarefa, "cpf": cpf})
                    conn.commit()
                    print(f"\n {margem} Tarefa excluída com sucesso!\n")
                else:
                    print(f"\n {margem} Exclusão cancelada.\n")
                input("\nPressione ENTER para continuar...")
                if not perguntar_continuar2(f"excluir outra tarefa do colaborador {nome}"):
                    break 
        except Exception as e:
            conn.rollback()
            print(f"\n {margem} Erro ao excluir tarefa: {e}\n")
            input("\nPressione ENTER para continuar...")
        finally:
            cursor.close()
        if not perguntar_continuar2("excluir tarefa de outro colaborador"):
            break  

# COLABORADOR 

def listar_tarefas_colaborador(conn: oracledb.Connection, cpf_colaborador: str) -> None:
    """
    Busca e exibe todas as tarefas PENDENTES/EM ANDAMENTO de um colaborador específico.
    A função utiliza o CPF do colaborador logado para filtrar as tarefas na tabela
    T_MNDSH_TAREFA, excluindo aquelas com status 'concluída'. Os resultados são exibidos
    em formato de tabela e podem ser exportados.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.
        cpf_colaborador: O CPF do colaborador logado.

    Returns:
        None: A função gerencia a exibição e exportação.

    Dependências:
        - Funções: limpa_tela, imprimir_tabela, gerar_dataframe.
        - Variáveis: margem.
        - Módulo: pandas (pd).
    """
    limpa_tela()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT nm_colaborador FROM T_MNDSH_COLABORADOR WHERE nr_cpf = :cpf", {"cpf": cpf_colaborador})
        resultado = cursor.fetchone()
        if not resultado:
            print("\nErro ao identificar colaborador.\n")
            input("Pressione ENTER para voltar...")
            return
        nome = resultado[0]

        cursor.execute("""
            SELECT id_tarefa, ds_titulo, ds_descricao, ds_status, ds_prioridade, dt_prazo, dt_criacao, dt_modificacao
            FROM T_MNDSH_TAREFA
            WHERE nr_cpf = :cpf AND ds_status != 'concluída'
            ORDER BY dt_prazo
        """, {"cpf": cpf_colaborador})
        tarefas = cursor.fetchall()

        if not tarefas:
            print(f"\n{margem} Nenhuma tarefa encontrada para {nome} (CPF: {cpf_colaborador}).\n")
            input("Pressione ENTER para voltar...")
            return
        df = pd.DataFrame(tarefas, columns=["ID", "Título", "Descrição", "Status", "Prioridade", "Prazo", "Data Criação", "Data Última Modificação"])
        imprimir_tabela(df, 
                        titulo=f"TAREFAS DE {nome}", 
                        tamanhos_wrap={"Título":25,"Descrição":40}, 
                        colunas_datas=["Prazo"], 
                        colunas_datetime=["Data Criação", "Data Última Modificação"])
        gerar_dataframe(df)
    except Exception as e:
        print(f"\n{margem} Erro ao listar tarefas: {e}\n")
        input("Pressione ENTER para continuar...")
    finally:
        cursor.close()

def atualizar_tarefa_colaborador(conn: oracledb.Connection, cpf_colaborador: str, nome_colaborador: str) -> None:
    """
    Permite ao colaborador alterar o status de suas tarefas (para 'em andamento' ou 'concluída').
    A função lista apenas as tarefas NÃO CONCLUÍDAS do colaborador. Após a seleção do ID,
    permite alterar o status, validando que a tarefa pertence ao colaborador e não está
    concluída. A alteração é confirmada e registrada no banco.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.
        cpf_colaborador: O CPF do colaborador logado.
        nome_colaborador: O nome do colaborador logado.

    Returns:
        None: A função realiza a ação de atualização.

    Dependências:
        - Funções: limpa_tela, imprimir_tabela, menu_opcoes2, perguntar_continuar,
                   perguntar_continuar2.
        - Variáveis: margem.
        - Módulo: pandas (pd).
    """
    while True:
        limpa_tela()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id_tarefa, ds_titulo, ds_descricao, ds_status, ds_prioridade, dt_prazo, dt_criacao, dt_modificacao
                FROM T_MNDSH_TAREFA
                WHERE nr_cpf = :cpf AND ds_status != 'concluída'
                ORDER BY dt_prazo
            """, {"cpf": cpf_colaborador})
            tarefas = cursor.fetchall()
            if not tarefas:
                limpa_tela()
                print(f"===== TAREFAS A CONCLUIR DE {nome_colaborador} (CPF: {cpf_colaborador}) =====")
                print(f"\n{margem} Nenhuma tarefa NÃO CONCLUÍDA encontrada.\n")
                input("Pressione ENTER para voltar...")
                break

            df = pd.DataFrame(tarefas, columns=["ID", "Título", "Descrição", "Status", "Prioridade", "Prazo", "Data Criação", "Data Última Modificação"])
            imprimir_tabela(df, 
                            titulo=f"TAREFAS A CONCLUIR DE {nome_colaborador}",
                            tamanhos_wrap={"Título":25,"Descrição":40},  
                            colunas_datas=["Prazo"], 
                            colunas_datetime=["Data Criação","Data Última Modificação"])
            
            id_tarefa = None
            while True:
                try:
                    id_tarefa_input = input("\nDigite o ID da tarefa que deseja ATUALIZAR: ")
                    id_tarefa = int(id_tarefa_input)
                    break 
                except ValueError:
                    print(f"\n{margem} Entrada inválida. Por favor, digite o número do ID.\n")
                    if not perguntar_continuar("tentar novamente"):
                        id_tarefa = -1
                        break
            if id_tarefa == -1:
                break

            cursor.execute("""
                SELECT id_tarefa, ds_titulo, ds_descricao, ds_status, ds_prioridade, dt_prazo, dt_criacao, dt_modificacao
                FROM T_MNDSH_TAREFA
                WHERE id_tarefa=:id AND nr_cpf=:cpf_colaborador AND ds_status != 'concluída'
            """, {"id": id_tarefa, "cpf_colaborador": cpf_colaborador})
            tarefa_completa = cursor.fetchone()

            if not tarefa_completa:
                print(f"\n{margem} ID da tarefa ({id_tarefa}) é inválido, já está concluído ou não pertence à sua lista.\n")
                input("Pressione ENTER para continuar...")
                continue

            limpa_tela()
            df_tarefa = pd.DataFrame([tarefa_completa], columns=["ID","Título","Descrição","Status","Prioridade","Prazo","Data Criação","Última Modificação"])
            imprimir_tabela(df_tarefa, 
                            titulo=f"ATUALIZAR TAREFA - {nome_colaborador}", 
                            tamanhos_wrap={"Título":25,"Descrição":40}, 
                            colunas_datas=["Prazo"], 
                            colunas_datetime=["Data Criação","Última Modificação"])

            novo_status = menu_opcoes2("\nSelecione o novo status:", ["Em Andamento", "Concluída", "Voltar"], ["EM ANDAMENTO", "CONCLUIDA", "VOLTAR"])

            if novo_status == "VOLTAR":
                print(f"\n{margem} Atualização cancelada.\n")
                input("Pressione ENTER para continuar...")
                continue
            
            status_bd = 'em andamento' if novo_status == 'EM ANDAMENTO' else 'concluída'
            status_display = novo_status.upper()

            confirmacao = perguntar_continuar(f"confirmar a mudança para o status '{status_display}'")
            if confirmacao:
                cursor.execute("""
                    UPDATE T_MNDSH_TAREFA 
                    SET ds_status=:novo_status, dt_modificacao=SYSDATE 
                    WHERE id_tarefa=:id AND nr_cpf=:cpf_colaborador
                """, {"novo_status": status_bd, "id": id_tarefa, "cpf_colaborador": cpf_colaborador})
                
                conn.commit()
                print(f"\n{margem} Tarefa '{tarefa_completa[1]}' atualizada para status: {status_display} com sucesso!\n")
            else:
                continue
            if not perguntar_continuar2("atualizar outra tarefa"):
                        break
        except Exception as e:
            conn.rollback()
            print(f"\n{margem} Erro ao atualizar tarefa: {e}\n")
            input("Pressione ENTER para voltar...")
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()

# ====== Registro de métricas e relatórios ======

def registrar_metrica(conn: oracledb.Connection, cpf_colaborador: str) -> None:
    """
    Permite ao colaborador registrar suas métricas diárias em cinco categorias:
    Produtividade, Bem-estar emocional, Satisfação no trabalho, Qualidade do sono e Bem-estar físico.

    A função primeiro verifica se já existe um registro para o colaborador na data atual (SYSDATE).
    Em caso negativo, ela calcula métricas objetivas de tarefas e, em seguida, solicita notas subjetivas 
    (0 a 10) para cada categoria,de maneira que se colaborador não responder ou não responder corretamente 
    ele fica preso no loop ate que a resposta certa seja dada obrigando assim o colaborador a responder 
    obrigatoriamente todas as questões, realizando múltiplas inserções na tabela T_MNDSH_METRICA.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.
        cpf_colaborador: O CPF do colaborador logado.

    Returns:
        None: A função realiza as inserções no banco.

    Dependências:
        - Funções: limpa_tela, valida_nota.
        - Variáveis: margem.
    """
    limpa_tela()
    print("===== REGISTRAR MÉTRICAS =====\n")
    print(f"{margem}Registro obrigatório diario (de preferência ao fim do expediente).")
    print(f"\n{margem}Respostas apenas números inteiros de 0 a 10.")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 1
            FROM T_MNDSH_METRICA
            WHERE nr_cpf = :cpf
              AND TRUNC(dt_registro) = TRUNC(SYSDATE)
        """, {"cpf": cpf_colaborador})
        if cursor.fetchone():
            print(f"\n{margem}AVISO: Já existe um registro de métricas para o colaborador {cpf_colaborador} na data de hoje.")
            print(f"{margem}Por favor, retorne amanhã para um novo registro.")
            return 

        cursor.execute("""
            SELECT
                SUM(CASE WHEN ds_status = 'concluída' THEN 1 ELSE 0 END),
                SUM(CASE WHEN ds_status = 'em andamento' THEN 1 ELSE 0 END),
                SUM(CASE WHEN ds_status = 'pendente' THEN 1 ELSE 0 END),
                SUM(CASE WHEN ds_status = 'concluída' AND TRUNC(dt_prazo) >= TRUNC(SYSDATE) THEN 1 ELSE 0 END),
                SUM(CASE WHEN ds_status = 'concluída' AND TRUNC(dt_prazo) < TRUNC(SYSDATE) THEN 1 ELSE 0 END)
            FROM T_MNDSH_TAREFA
            WHERE nr_cpf = :cpf
        """, {"cpf": cpf_colaborador})
        r = cursor.fetchone()
        tarefas_concluidas = int(r[0] or 0)
        tarefas_andamento = int(r[1] or 0)
        tarefas_pendentes = int(r[2] or 0)
        concluidas_no_prazo = int(r[3] or 0)
        concluidas_atraso = int(r[4] or 0)

        print("\n--- PRODUTIVIDADE ---")
        print(f"\nTarefas concluídas: {tarefas_concluidas}, Em andamento: {tarefas_andamento}, Pendentes: {tarefas_pendentes}")
        print(f"\nConcluídas no prazo: {concluidas_no_prazo}, Atrasadas: {concluidas_atraso}")

        horas_produtivas = valida_nota("\nHoras produtivas hoje (0-10): ")
        nivel_foco = valida_nota("\nNível de foco (0-10): ")

        cursor.execute("""
            INSERT INTO T_MNDSH_METRICA (nr_cpf, tipo_metrica, dt_registro,horas_produtivas, nivel_foco, 
                       tarefas_concluidas, tarefas_andamento, tarefas_pendentes,concluidas_no_prazo, concluidas_atraso)
            VALUES (:cpf, 'Produtividade', SYSDATE,:horas_produtivas, :nivel_foco, :tarefas_concluidas, :tarefas_andamento, 
                       :tarefas_pendentes,:concluidas_no_prazo, :concluidas_atraso)
        """, {"cpf": cpf_colaborador, "horas_produtivas": horas_produtivas, "nivel_foco": nivel_foco, "tarefas_concluidas": tarefas_concluidas,
            "tarefas_andamento": tarefas_andamento, "tarefas_pendentes": tarefas_pendentes, "concluidas_no_prazo": concluidas_no_prazo, "concluidas_atraso": concluidas_atraso})

        # --- BEM-ESTAR EMOCIONAL ---
        print("\n--- BEM-ESTAR EMOCIONAL ---")
        estresse = valida_nota("\nNível de estresse (0-10): ")
        humor = valida_nota("\nHumor geral (0-10): ")
        energia = valida_nota("\nEnergia/disposição (0-10): ")
        controle_dia = valida_nota("\nSensação de controle sobre o dia (0-10): ")

        cursor.execute("""
            INSERT INTO T_MNDSH_METRICA (nr_cpf, tipo_metrica, dt_registro, estresse, humor, energia, controle_dia)
            VALUES (:cpf, 'Bem-estar emocional', SYSDATE, :estresse, :humor, :energia, :controle_dia)
        """, {"cpf": cpf_colaborador, "estresse": estresse, "humor": humor, "energia": energia, "controle_dia": controle_dia})

        # --- SATISFAÇÃO NO TRABALHO ---
        print("\n--- SATISFAÇÃO NO TRABALHO ---")
        satisfacao_geral = valida_nota("\nSatisfação geral (0-10): ")
        relacao_colegas = valida_nota("\nRelação com colegas (0-10): ")
        reconhecimento = valida_nota("\nReconhecimento recebido (0-10): ")
        carga_trabalho = valida_nota("\nCarga de trabalho percebida (0-10): ")

        cursor.execute("""
            INSERT INTO T_MNDSH_METRICA (nr_cpf, tipo_metrica, dt_registro, satisfacao_geral, relacao_colegas, reconhecimento, carga_trabalho)
            VALUES (:cpf, 'Satisfação no trabalho', SYSDATE, :satisfacao_geral, :relacao_colegas, :reconhecimento, :carga_trabalho)
        """, {"cpf": cpf_colaborador, "satisfacao_geral": satisfacao_geral, "relacao_colegas": relacao_colegas, "reconhecimento": reconhecimento, "carga_trabalho": carga_trabalho})

        # --- QUALIDADE DO SONO ---
        print("\n--- QUALIDADE DO SONO ---")
        horas_dormidas = valida_nota("\nHoras dormidas (0-10): ")
        descanso = valida_nota("\nSensação de descanso (0-10): ")
        despertares = valida_nota("\nNúmero de vezes que acordou à noite (0-10): ")

        cursor.execute("""
            INSERT INTO T_MNDSH_METRICA (nr_cpf, tipo_metrica, dt_registro, horas_dormidas, descanso, despertares)
            VALUES (:cpf, 'Qualidade do sono', SYSDATE, :horas_dormidas, :descanso, :despertares)
        """, {"cpf": cpf_colaborador, "horas_dormidas": horas_dormidas, "descanso": descanso,"despertares": despertares})

        # --- BEM-ESTAR FÍSICO ---
        print("\n--- BEM-ESTAR FÍSICO ---")
        atividade_fisica = valida_nota("\nHoras de atividade física (0-10): ")
        ingestao_agua = valida_nota("\nIngestão de água (0-10): ")
        intensidade_atividade = valida_nota("\nIntensidade da atividade física (0-10): ")

        cursor.execute("""
            INSERT INTO T_MNDSH_METRICA (nr_cpf, tipo_metrica, dt_registro, atividade_fisica, ingestao_agua, intensidade_atividade)
            VALUES (:cpf, 'Bem-estar físico', SYSDATE, :atividade_fisica, :ingestao_agua, :intensidade_atividade)
        """, {"cpf": cpf_colaborador, "atividade_fisica": atividade_fisica, "ingestao_agua": ingestao_agua, "intensidade_atividade": intensidade_atividade})

        conn.commit()
        print(f"\n{margem}Métricas registradas com sucesso!\n")
    except Exception as e:
        print(f"\n{margem}Erro ao registrar métricas: {e}\n")
        conn.rollback()
    finally:
        cursor.close()
        input("\nPressione ENTER para voltar ao menu do colaborador...")
        limpa_tela()

# ====== FUNÇÕES DE CÁLCULO E FEEDBACK ======

def bom(valor: int | float, metrica: str) -> bool:
    """
    Verifica se o valor de uma métrica é considerado BOM (>= 7 ou <= 4).

    A natureza da métrica (positiva ou negativa) é consultada no dicionário 
    global metrica_positiva_negativa.

    - Métrica 'positiva'  BOM se o valor for >= 7.
    - Métrica 'negativa'  BOM se o valor for <= 4.

    Args:
        valor: O valor numérico da métrica.
        metrica: A chave string da métrica.

    Returns:
        bool: True se o valor for considerado BOM, False caso contrário.
    """
    if metrica_positiva_negativa.get(metrica, "positivo") == "positivo":
        return valor >= 7
    else:
        return valor <= 4

def ruim(valor: int | float, metrica: str) -> bool:
    """
    Verifica se o valor de uma métrica é considerado RUIM (<= 4 ou >= 7).

    A natureza da métrica (positiva ou negativa) é consultada no dicionário 
    global metrica_positiva_negativa.

    - Métrica 'positiva' RUIM se o valor for <= 4.
    - Métrica 'negativa' RUIM se o valor for >= 7.

    Args:
        valor: O valor numérico da métrica.
        metrica: A chave string da métrica.

    Returns:
        bool: True se o valor for considerado RUIM, False caso contrário.
    """
    if metrica_positiva_negativa.get(metrica, "positivo") == "positivo":
        return valor <= 4
    else:
        return valor >= 7

def adiciona_insight(cond: bool, text: str, insights: list) -> None:
    """
    Adiciona uma string de feedback à lista de insights se a condição for verdadeira.

    Args:
        cond: Condição booleana que, se True, dispara a adição do insight.
        text: A mensagem de feedback a ser adicionada.
        insights: A lista de strings de insights, passada por referência.

    Returns:
        None: Modifica a lista 'insights' diretamente.
    """
    if cond:
        insights.append(f"\n {margem} {text}\n")

def gerar_feedback_e_insights(metricas: dict) -> tuple[str, list[str]]:
    """
    Gera feedback textual e uma lista de insights acionáveis baseados nas métricas individuais do colaborador.

    A função calcula a proporção de conclusão de tarefas e, em seguida, aplica lógica de correlação
    (usando as funções bom() e ruim()) para identificar relações entre métricas.

    Args:
        metricas: Dicionário contendo as métricas de um único colaborador.

    Returns:
        tuple[str, list[str]]: Uma tupla contendo:
        1. Uma string formatada de feedback resumido.
        2. Uma lista de strings de insights gerados por correlação.

    Dependências:
        - Funções: bom, ruim, adiciona_insight.
        - Variáveis: margem.
    """
    if not metricas:
        mensagem = f"\n {margem} Sem métricas disponíveis.\n"
        return mensagem, []

    feedback = []
    insights = []

    m = metricas 
    produtividade = m.get("produtividade", 0)
    foco = m.get("foco", 0)
    estresse = m.get("estresse", 0)
    humor = m.get("humor", 0)
    energia = m.get("energia", 0)
    controle_dia = m.get("controle_dia", 0)
    satisfacao = m.get("satisfacao", 0)
    relacao_colegas = m.get("relacao_colegas", 0)
    reconhecimento = m.get("reconhecimento", 0)
    carga_trabalho = m.get("carga_trabalho", 0)
    sono_horas = m.get("sono_horas", 0)
    sono_descanso = m.get("sono_descanso", 0)
    despertares = m.get("despertares", 0)
    agua = m.get("agua", 0)
    intensidade_atividade = m.get("intensidade_atividade", 0)
    atividade_fisica = m.get("atividade_fisica", 0)
    tarefas_concluidas = m.get("tarefas_concluidas", 0)
    tarefas_andamento = m.get("tarefas_andamento", 0)
    tarefas_pendentes = m.get("tarefas_pendentes", 0)

    total_tarefas = tarefas_concluidas + tarefas_andamento + tarefas_pendentes
    prop_conclusao = tarefas_concluidas / total_tarefas if total_tarefas > 0 else 0
    feedback.append(f"\n {margem} Proporção de tarefas concluídas: {prop_conclusao:.0%}\n")

    if bom(produtividade, "produtividade") and bom(foco, "foco"):
        feedback.append(f"\n {margem} Excelente produtividade e foco!\n")
    elif produtividade >= 5 or foco >= 5:
        feedback.append(f"\n {margem} Bom desempenho, mas há espaço para melhorias.\n")
    else:
        feedback.append(f"\n {margem} Necessita atenção ao cumprimento das tarefas.\n")

    feedback.append(f"\n {margem} Nível de estresse: {estresse:.2f}, Humor: {humor:.2f}, Energia: {energia:.2f}, Controle do dia: {controle_dia:.2f}\n")
    feedback.append(f"\n {margem} Satisfação: {satisfacao:.2f}, Relação com colegas: {relacao_colegas:.2f}, Reconhecimento: {reconhecimento:.2f}, Carga de trabalho: {carga_trabalho:.2f}\n")
    feedback.append(f"\n {margem} Sono - Horas: {sono_horas:.2f}, Descanso: {sono_descanso:.2f}, Despertares: {despertares:.2f}\n")
    feedback.append(f"\n {margem} Atividade física: {atividade_fisica:.2f}, Consumo de água: {agua:.2f}, Intensidade da atividade: {intensidade_atividade:.2f}\n")

    # Proporção de conclusão
    adiciona_insight(prop_conclusao < 0.5, "Baixa proporção de tarefas concluídas; atenção à priorização e gerenciamento do tempo.", insights)
    adiciona_insight(prop_conclusao >= 0.8, "Alta proporção de tarefas concluídas, bom desempenho consistente.", insights)

    # Produtividade x proporção de conclusão
    adiciona_insight(bom(produtividade, "produtividade") and prop_conclusao < 0.5,
                "Alta produtividade reportada, mas baixa proporção de conclusão; revisar eficiência.", insights)
    adiciona_insight(ruim(produtividade, "produtividade") and prop_conclusao >= 0.8,
                "Produtividade baixa, mas proporção de tarefas concluídas boa; atenção ao ritmo de trabalho.", insights)

    # Produtividade x sono
    adiciona_insight(bom(produtividade, "produtividade") and (ruim(sono_horas, "sono_horas") or ruim(sono_descanso, "sono_descanso")),
                "Produtividade está boa, mas sono ruim pode afetar resultados futuros.", insights)
    adiciona_insight(ruim(produtividade, "produtividade") and (ruim(sono_horas, "sono_horas") or ruim(sono_descanso, "sono_descanso")),
                "Baixa produtividade possivelmente ligada à qualidade do sono.", insights)

    # Produtividade x estresse
    adiciona_insight(ruim(produtividade, "produtividade") and ruim(estresse, "estresse"),
                "Baixa produtividade associada a alto estresse.", insights)

    # Energia x atividade física
    adiciona_insight(ruim(energia, "energia") and ruim(atividade_fisica, "atividade_fisica"),
                "Baixa energia e pouca atividade física. Pode afetar desempenho e saúde.", insights)
    adiciona_insight(bom(energia, "energia") and ruim(atividade_fisica, "atividade_fisica"),
                "Energia alta apesar de pouca atividade física; cuidado para manter bem-estar físico.", insights)

    # Satisfação x reconhecimento
    adiciona_insight(ruim(satisfacao, "satisfacao") and ruim(reconhecimento, "reconhecimento"),
                "Baixa satisfação e reconhecimento. Atenção à motivação no trabalho.", insights)
    adiciona_insight(bom(satisfacao, "satisfacao") and ruim(reconhecimento, "reconhecimento"),
                "Satisfação alta apesar de pouco reconhecimento; monitorar engajamento.", insights)

    # Água x energia
    adiciona_insight(ruim(agua, "agua") and ruim(energia, "energia"),
                "Baixa ingestão de água correlacionada com pouca energia.", insights)

    # Humor x controle do dia
    adiciona_insight(ruim(humor, "humor") and ruim(controle_dia, "controle_dia"),
                "Humor baixo possivelmente associado a sensação de pouco controle sobre o dia.", insights)
    adiciona_insight(bom(humor, "humor") and ruim(controle_dia, "controle_dia"),
                "Humor bom, mas sensação de pouco controle sobre o dia pode gerar estresse futuro.", insights)

    # Sono x energia
    adiciona_insight(ruim(sono_horas, "sono_horas") and bom(energia, "energia"),
                "Boa energia apesar de pouco sono; atenção à fadiga futura.", insights)
    adiciona_insight(ruim(sono_horas, "sono_horas") and ruim(energia, "energia"),
                "Pouco sono e baixa energia; risco de queda de desempenho.", insights)

    # Estresse x humor x energia
    adiciona_insight(ruim(estresse, "estresse") and ruim(humor, "humor") and ruim(energia, "energia"),
                "Alto estresse, humor baixo e energia baixa: atenção ao bem-estar emocional.", insights)

    # Atividade física x energia
    adiciona_insight(ruim(atividade_fisica, "atividade_fisica") and ruim(energia, "energia"),
                "Baixa atividade física e energia reduzida, cuidado com saúde geral.", insights)

    # Hidratação x energia
    adiciona_insight(ruim(agua, "agua") and ruim(energia, "energia"),
                "Baixa ingestão de água e energia baixa, atenção à hidratação.", insights)

    # Sono x estresse
    adiciona_insight(ruim(sono_horas, "sono_horas") and ruim(estresse, "estresse"),
                "Sono insuficiente e alto estresse; risco elevado de burnout.", insights)

    # Carga de trabalho x estresse
    adiciona_insight(ruim(carga_trabalho, "carga_trabalho") and ruim(estresse, "estresse"),
                "Alta carga de trabalho correlacionada com estresse elevado.", insights)

    # Produtividade x satisfação
    adiciona_insight(bom(produtividade, "produtividade") and ruim(satisfacao, "satisfacao"),
                "Produtividade boa, mas baixa satisfação; monitorar motivação.", insights)

    # Combinadas adicionais
    adiciona_insight(prop_conclusao < 0.5 and ruim(energia, "energia"),
                "Baixa conclusão de tarefas e pouca energia; atenção ao gerenciamento de tempo e bem-estar.", insights)
    adiciona_insight(ruim(estresse, "estresse") and ruim(sono_horas, "sono_horas"),
                "Estresse elevado e pouco sono; risco de fadiga e burnout.", insights)
    adiciona_insight(ruim(carga_trabalho, "carga_trabalho") and ruim(energia, "energia"),
                "Carga de trabalho alta com baixa energia; atenção à sobrecarga.", insights)
    adiciona_insight(ruim(atividade_fisica, "atividade_fisica") and ruim(sono_horas, "sono_horas"),
                "Pouca atividade física e sono insuficiente; risco de redução de desempenho físico e mental.", insights)
    adiciona_insight(ruim(agua, "agua") and ruim(carga_trabalho, "carga_trabalho"),
                "Baixa ingestão de água com alta carga de trabalho; cuidado com hidratação e estresse.", insights)
    adiciona_insight(ruim(sono_horas, "sono_horas") and ruim(carga_trabalho, "carga_trabalho"),
                "Pouco sono e alta carga de trabalho; risco de burnout.", insights)
    adiciona_insight(bom(humor, "humor") and ruim(reconhecimento, "reconhecimento"),
                "Humor bom mesmo com baixo reconhecimento; monitorar motivação futura.", insights)
    adiciona_insight(ruim(humor, "humor") and bom(reconhecimento, "reconhecimento"),
                "Mau humor mesmo com reconhecimento alto; atenção a fatores externos.", insights)
    adiciona_insight(bom(energia, "energia") and ruim(sono_horas, "sono_horas") and ruim(atividade_fisica, "atividade_fisica"),
                "Alta energia com pouco sono e pouca atividade física; risco de fadiga futura.", insights)
    adiciona_insight(ruim(satisfacao, "satisfacao") and ruim(estresse, "estresse") and ruim(carga_trabalho, "carga_trabalho"),
                "Baixa satisfação, alto estresse e alta carga de trabalho; alerta vermelho de desmotivação.", insights)
    adiciona_insight(bom(produtividade, "produtividade") and ruim(energia, "energia"),
                "Produtividade alta, mas energia baixa; risco de queda de desempenho.", insights)

    return "\n".join(feedback), insights

def gerar_feedback_e_insights_geral(metricas_media: dict) -> tuple[str, list[str]]:
    """
    Gera feedback textual e uma lista de insights acionáveis baseados nas métricas médias da equipe.

    A função aplica a mesma lógica de correlação da análise individual, mas usando valores médios
    para identificar tendências e problemas coletivos que o gestor deve abordar.

    Args:
        metricas_media: Dicionário contendo as médias das métricas de toda a equipe.

    Returns:
        tuple[str, list[str]]: Uma tupla contendo:
        1. Uma string formatada de feedback resumido (médias da equipe).
        2. Uma lista de strings de insights coletivos gerados por correlação.

    Dependências:
        - Funções: bom, ruim, adiciona_insight.
        - Variáveis: margem.
    """
    if not metricas_media:
        return f"\n {margem} Sem métricas disponíveis.\n", []

    feedback = []
    insights = []

    m = metricas_media

    produtividade = m.get("produtividade", 0)
    foco = m.get("foco", 0)
    estresse = m.get("estresse", 0)
    humor = m.get("humor", 0)
    energia = m.get("energia", 0)
    controle_dia = m.get("controle_dia", 0)
    satisfacao = m.get("satisfacao", 0)
    relacao_colegas = m.get("relacao_colegas", 0)
    reconhecimento = m.get("reconhecimento", 0)
    carga_trabalho = m.get("carga_trabalho", 0)
    sono_horas = m.get("sono_horas", 0)
    sono_descanso = m.get("sono_descanso", 0)
    despertares = m.get("despertares", 0)
    agua = m.get("agua", 0)
    intensidade_atividade = m.get("intensidade_atividade", 0)
    atividade_fisica = m.get("atividade_fisica", 0)
    prop_conclusao = m.get("prop_conclusao", 0)
    tarefas_concluidas = m.get("tarefas_concluidas", 0)
    tarefas_andamento = m.get("tarefas_andamento", 0)
    tarefas_pendentes = m.get("tarefas_pendentes", 0) 

    total_tarefas = tarefas_concluidas + tarefas_andamento + tarefas_pendentes
    prop_conclusao = tarefas_concluidas / total_tarefas if total_tarefas > 0 else 0
    feedback.append(f"\n {margem} Proporção média de tarefas concluídas: {prop_conclusao:.0%}\n")
    
    if produtividade >= 7 and foco >= 7:
        feedback.append(f"\n {margem} Produtividade e foco médios da equipe estão excelentes.\n")
    elif produtividade >= 5 or foco >= 5:
        feedback.append(f"\n {margem} Produtividade e foco médios da equipe estão bons, mas há espaço para melhorias.\n")
    else:
        feedback.append(f"\n {margem} A equipe precisa melhorar o cumprimento das tarefas.\n")
    
    feedback.append(f"\n {margem} Médias da equipe - Estresse: {estresse:.2f}, Humor: {humor:.2f}, Energia: {energia:.2f}, Controle do dia: {controle_dia:.2f}\n")
    feedback.append(f"\n {margem} Satisfação: {satisfacao:.2f}, Relação com colegas: {relacao_colegas:.2f}, Reconhecimento: {reconhecimento:.2f}, Carga de trabalho: {carga_trabalho:.2f}\n")
    feedback.append(f"\n {margem} Sono - Horas: {sono_horas:.2f}, Descanso: {sono_descanso:.2f}, Despertares: {despertares:.2f}\n")
    feedback.append(f"\n {margem} Atividade física: {atividade_fisica:.2f}, Consumo de água: {agua:.2f}, Intensidade da atividade: {intensidade_atividade:.2f}\n")

    # Proporção de conclusão
    adiciona_insight(prop_conclusao < 0.5, "Em média, menos da metade das tarefas são concluídas; atenção à priorização e gestão de tempo.", insights)
    adiciona_insight(prop_conclusao >= 0.8, "Em média, a proporção de tarefas concluídas é alta; equipe com bom desempenho consistente.", insights)

    # Produtividade x proporção de conclusão
    adiciona_insight(bom(produtividade, "produtividade") and prop_conclusao < 0.5,
                "Produtividade média alta, mas proporção de conclusão de tarefas baixa; revisar eficiência da equipe.", insights)
    adiciona_insight(ruim(produtividade, "produtividade") and prop_conclusao >= 0.8,
                "Produtividade média baixa, mas equipe consegue concluir tarefas; atenção ao ritmo de trabalho.", insights)

    # Produtividade x sono
    adiciona_insight(bom(produtividade, "produtividade") and (ruim(sono_horas, "sono_horas") or ruim(sono_descanso, "sono_descanso")),
                "Produtividade média boa, mas qualidade do sono da equipe pode impactar resultados futuros.", insights)
    adiciona_insight(ruim(produtividade, "produtividade") and (ruim(sono_horas, "sono_horas") or ruim(sono_descanso, "sono_descanso")),
                "Produtividade média baixa possivelmente ligada à qualidade do sono da equipe.", insights)

    # Produtividade x estresse
    adiciona_insight(ruim(produtividade, "produtividade") and ruim(estresse, "estresse"),
                "Produtividade média baixa associada a alto estresse na equipe.", insights)

    # Energia x atividade física
    adiciona_insight(ruim(energia, "energia") and ruim(atividade_fisica, "atividade_fisica"),
                "Baixa energia e pouca atividade física média da equipe; pode afetar desempenho e saúde.", insights)
    adiciona_insight(bom(energia, "energia") and ruim(atividade_fisica, "atividade_fisica"),
                "Energia média boa apesar de pouca atividade física; monitorar bem-estar físico da equipe.", insights)

    # Satisfação x reconhecimento
    adiciona_insight(ruim(satisfacao, "satisfacao") and ruim(reconhecimento, "reconhecimento"),
                "Satisfação e reconhecimento médios da equipe baixos; atenção à motivação coletiva.", insights)
    adiciona_insight(bom(satisfacao, "satisfacao") and ruim(reconhecimento, "reconhecimento"),
                "Satisfação média boa apesar de pouco reconhecimento; monitorar engajamento da equipe.", insights)

    # Água x energia
    adiciona_insight(ruim(agua, "agua") and ruim(energia, "energia"),
                "Baixa ingestão de água correlacionada com baixa energia média da equipe.", insights)

    # Humor x controle do dia
    adiciona_insight(ruim(humor, "humor") and ruim(controle_dia, "controle_dia"),
                "Humor médio baixo possivelmente associado à sensação de pouco controle sobre o dia.", insights)
    adiciona_insight(bom(humor, "humor") and ruim(controle_dia, "controle_dia"),
                "Humor médio bom, mas sensação de pouco controle sobre o dia pode gerar estresse futuro.", insights)

    # Sono x energia
    adiciona_insight(ruim(sono_horas, "sono_horas") and bom(energia, "energia"),
                "Boa energia média da equipe apesar de pouco sono; atenção à fadiga futura.", insights)
    adiciona_insight(ruim(sono_horas, "sono_horas") and ruim(energia, "energia"),
                "Pouco sono e baixa energia média; risco de queda de desempenho coletivo.", insights)

    # Estresse x humor x energia
    adiciona_insight(ruim(estresse, "estresse") and ruim(humor, "humor") and ruim(energia, "energia"),
                "Alto estresse, humor baixo e energia baixa médios; atenção ao bem-estar emocional da equipe.", insights)

    # Atividade física x energia
    adiciona_insight(ruim(atividade_fisica, "atividade_fisica") and ruim(energia, "energia"),
                "Baixa atividade física e energia reduzida na média; cuidado com saúde geral da equipe.", insights)

    # Hidratação x energia
    adiciona_insight(ruim(agua, "agua") and ruim(energia, "energia"),
                "Baixa ingestão de água e energia baixa média; atenção à hidratação da equipe.", insights)

    # Sono x estresse
    adiciona_insight(ruim(sono_horas, "sono_horas") and ruim(estresse, "estresse"),
                "Sono insuficiente e alto estresse médio; risco elevado de burnout coletivo.", insights)

    # Carga de trabalho x estresse
    adiciona_insight(ruim(carga_trabalho, "carga_trabalho") and ruim(estresse, "estresse"),
                "Alta carga de trabalho correlacionada com estresse elevado médio da equipe.", insights)

    # Produtividade x satisfação
    adiciona_insight(bom(produtividade, "produtividade") and ruim(satisfacao, "satisfacao"),
                "Produtividade média boa, mas satisfação baixa; monitorar motivação da equipe.", insights)

    # Combinadas adicionais
    adiciona_insight(prop_conclusao < 0.5 and ruim(energia, "energia"),
                "Baixa conclusão de tarefas e pouca energia média; atenção à gestão de tempo e bem-estar coletivo.", insights)
    adiciona_insight(ruim(estresse, "estresse") and ruim(sono_horas, "sono_horas"),
                "Estresse elevado e pouco sono médio; risco de fadiga e burnout.", insights)
    adiciona_insight(ruim(carga_trabalho, "carga_trabalho") and ruim(energia, "energia"),
                "Carga de trabalho alta com baixa energia média; atenção à sobrecarga da equipe.", insights)
    adiciona_insight(ruim(atividade_fisica, "atividade_fisica") and ruim(sono_horas, "sono_horas"),
                "Pouca atividade física e sono insuficiente médio; risco de redução de desempenho físico e mental coletivo.", insights)
    adiciona_insight(ruim(agua, "agua") and ruim(carga_trabalho, "carga_trabalho"),
                "Baixa ingestão de água com alta carga de trabalho média; cuidado com hidratação e estresse.", insights)
    adiciona_insight(ruim(sono_horas, "sono_horas") and ruim(carga_trabalho, "carga_trabalho"),
                "Pouco sono e alta carga de trabalho médio; risco de burnout coletivo.", insights)
    adiciona_insight(bom(humor, "humor") and ruim(reconhecimento, "reconhecimento"),
                "Humor médio bom mesmo com baixo reconhecimento; monitorar motivação futura da equipe.", insights)
    adiciona_insight(ruim(humor, "humor") and bom(reconhecimento, "reconhecimento"),
                "Humor médio baixo mesmo com reconhecimento alto; atenção a fatores externos.", insights)
    adiciona_insight(bom(energia, "energia") and ruim(sono_horas, "sono_horas") and ruim(atividade_fisica, "atividade_fisica"),
                "Alta energia média com pouco sono e pouca atividade física; risco de fadiga futura.", insights)
    adiciona_insight(ruim(satisfacao, "satisfacao") and ruim(estresse, "estresse") and ruim(carga_trabalho, "carga_trabalho"),
                "Baixa satisfação, alto estresse e alta carga de trabalho média; alerta vermelho de desmotivação.", insights)
    adiciona_insight(bom(produtividade, "produtividade") and ruim(energia, "energia"),
                "Produtividade alta, mas energia média baixa; risco de queda de desempenho coletivo.", insights)

    return "\n".join(feedback), insights

def calcular_desempenho(df_metrica: pd.DataFrame, data_filtro: pd.Timestamp = None) -> pd.DataFrame:
    """
    Processa um DataFrame de métricas brutas (T_MNDSH_METRICA) e consolida os dados, 
    calculando a média de cada variável numérica por colaborador (CPF) para um dia específico.

    Como cada métrica é inserida em linhas separadas no banco, esta função agrupa essas entradas
    pelo tipo de métrica e calcula a média para produzir um registro unificado por CPF para o dia.

    Args:
        df_metrica: DataFrame contendo as linhas de métricas brutas.
        data_filtro: Objeto Timestamp para filtrar os dados por data.

    Returns:
        pd.DataFrame: Um DataFrame consolidado onde cada linha representa o resumo
        de desempenho de um colaborador, com colunas nomeadas para as métricas.
    """
    if df_metrica.empty:
        return pd.DataFrame()

    df = df_metrica.copy()

    if data_filtro is not None:
        df["dt_registro"] = pd.to_datetime(df["dt_registro"], errors="coerce")
        df = df[df["dt_registro"].dt.date == data_filtro.date()]

    if df.empty:
        return pd.DataFrame()

    resultados = []

    for cpf, df_cpf in df.groupby("nr_cpf"):
        metricas = {"nr_cpf": cpf}
        for tipo, df_tipo in df_cpf.groupby("tipo_metrica"):
            valores = df_tipo.select_dtypes(include=["int", "float"]).mean().to_dict()
            if tipo == "Produtividade":
                metricas.update({
                    "produtividade": valores.get("horas_produtivas", 0),
                    "foco": valores.get("nivel_foco", 0),
                    "tarefas_concluidas": valores.get("tarefas_concluidas", 0),
                    "tarefas_andamento": valores.get("tarefas_andamento", 0),
                    "tarefas_pendentes": valores.get("tarefas_pendentes", 0),
                    "concluidas_no_prazo": valores.get("concluidas_no_prazo", 0),
                    "concluidas_atraso": valores.get("concluidas_atraso", 0)
                })
            elif tipo == "Bem-estar emocional":
                metricas.update({
                    "estresse": valores.get("estresse", 0),
                    "humor": valores.get("humor", 0),
                    "energia": valores.get("energia", 0),
                    "controle_dia": valores.get("controle_dia", 0)
                })
            elif tipo == "Satisfação no trabalho":
                metricas.update({
                    "satisfacao": valores.get("satisfacao_geral", 0),
                    "relacao_colegas": valores.get("relacao_colegas", 0),
                    "reconhecimento": valores.get("reconhecimento", 0),
                    "carga_trabalho": valores.get("carga_trabalho", 0)
                })
            elif tipo == "Qualidade do sono":
                metricas.update({
                    "sono_horas": valores.get("horas_dormidas", 0),
                    "sono_descanso": valores.get("descanso", 0),
                    "despertares": valores.get("despertares", 0)
                })
            elif tipo == "Bem-estar físico":
                metricas.update({
                    "atividade_fisica": valores.get("atividade_fisica", 0),
                    "agua": valores.get("ingestao_agua", 0)
                })

        resultados.append(metricas)
    return pd.DataFrame(resultados)

def relatorio_diario(conn: oracledb.Connection, cpf: str = None) -> None:
    """
    Gera o relatório diário de métricas para um colaborador específico, com base na data fornecida pelo usuário.

    A função interage com o usuário para obter a data, busca os dados brutos no banco,
    processa-os com calcular_desempenho(), formata o resultado e utiliza gerar_feedback_e_insights()
    para apresentar a tabela e os insights de forma organizada.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.
        cpf: O CPF do colaborador logado.

    Returns:
        None: Gerencia a interação com o usuário e a exibição do relatório.

    Dependências:
        - Funções: limpa_tela, validar_data, data_datetime, imprimir_tabela, gerar_dataframe, 
                   calcular_desempenho, gerar_feedback_e_insights, perguntar_continuar.
        - Variáveis: colunas_renomear, grupos_relatorio_diario, margem.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT nm_colaborador FROM T_MNDSH_COLABORADOR WHERE nr_cpf = :cpf", {"cpf": cpf})
    resultado = cursor.fetchone()
    nome = resultado[0] if resultado else None
    cursor.close()
    while True:
        limpa_tela()
        print(f"\n===== RELATÓRIO DIÁRIO - {nome} (CPF: {cpf})")
        data_str = input(f"\n{margem}Digite a data do relatório diário (DD/MM/AAAA): ").strip()
        if not validar_data(data_str):
            print(f"\n{margem}Data inválida!")
            if not perguntar_continuar("inserir data novamente"):
                return
            continue
        data_dt = data_datetime(data_str)
        break
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT *
            FROM T_MNDSH_METRICA
            WHERE nr_cpf = :cpf
              AND TRUNC(dt_registro) = TO_DATE(:data, 'DD/MM/YYYY')
        """, {"cpf": cpf, "data": data_str})
        linhas = cursor.fetchall()
        colunas = [d[0].lower() for d in cursor.description] if cursor.description else []
        cursor.close()
        if not linhas:
            print(f"\n{margem}Nenhuma métrica encontrada para essa data.")
            input("\nPressione ENTER para continuar...")
            return
        df_metrica = pd.DataFrame(linhas, columns=colunas)
    except Exception as e:
        print(f"\n{margem}Erro ao buscar métricas: {e}")
        input("\nPressione ENTER para continuar...")
        return
    
    df_desempenho = calcular_desempenho(df_metrica, data_dt)
    if df_desempenho.empty:
        print(f"\n{margem}Nenhum dado de desempenho calculado para esta data.")
        input("\nPressione ENTER para continuar...")
        return

    df_exibir = df_desempenho.rename(columns=colunas_renomear)
    colunas_numericas = df_exibir.select_dtypes(include="number").columns
    for col in colunas_numericas:
        df_exibir[col] = df_exibir[col].apply(lambda x: f"{x:.2f}")

    grupos_relatorio_diario = [
    ("PRODUTIVIDADE", ['Produtividade', 'Foco','Tarefas concluídas', 'Tarefas em andamento', 'Tarefas pendentes', 'Tarefas concluídas no prazo', 'Tarefas concluídas com atraso']),
    ("BEM-ESTAR EMOCIONAL", ['Estresse', 'Humor', 'Energia', 'Controle do dia']),
    ("SATISFAÇÃO NO TRABALHO", ['Satisfação', 'Relação colegas', 'Reconhecimento', 'Carga trabalho']),
    ("QUALIDADE DO SONO", ['Sono (horas)', 'Sono (descanso)', 'Despertares']),
    ("BEM-ESTAR FÍSICO", ['Atividade física', 'Água', 'Intensidade da atividade']),]


    if 'CPF' in df_exibir.columns:
        df_exibir = df_exibir.drop(columns=['CPF'])
    limpa_tela()
    data_formatada = data_dt.strftime("%d/%m/%Y")
    imprimir_tabela(df_exibir, 
                    titulo=f"Relatório Diário - {nome} CPF: {cpf} - Referência: {data_formatada}", 
                    colunas_exibir=grupos_relatorio_diario)
    feedback, insights = gerar_feedback_e_insights(df_desempenho.iloc[0].to_dict())
    print(feedback)
    for insight in insights:
        print(insight)
    gerar_dataframe(df_exibir)

def relatorio_mensal(conn: oracledb.Connection, cpf: str, nome: str, mes: int = None, ano: int = None) -> None:
    """
    Gera o relatório mensal de métricas para um colaborador específico.

    Busca todos os dados de métricas do colaborador para o mês e ano especificados,
    consolida-os em uma média mensal e exibe o resumo de desempenho e os insights individuais.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.
        cpf: O CPF do colaborador logado.
        nome: O nome do colaborador.
        mes: O mês de referência.
        ano: O ano de referência.

    Returns:
        None: Gerencia a interação com o usuário e a exibição do relatório.
    """
    while True:
        limpa_tela()
        print(f"\n===== RELATÓRIO MENSAL - {nome} (CPF: {cpf}) =====\n")

        if mes is None:
            mes_str = input(f"\n{margem}Digite o mês (1-12): ").strip()
        else:
            mes_str = str(mes)
        if ano is None:
            ano_str = input(f"\n{margem}Digite o ano (AAAA): ").strip()
        else:
            ano_str = str(ano)

        try:
            mes_int = int(mes_str)
            ano_int = int(ano_str)
            if 1 <= mes_int <= 12 and ano_int > 0:
                break
            print(f"\n{margem}Mês ou ano inválido!")
            if not perguntar_continuar("inserir mês e ano novamente"):
                return
        except ValueError:
            print(f"\n{margem}Mês ou ano inválido!")
            if not perguntar_continuar("inserir mês e ano novamente"):
                return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT *
            FROM T_MNDSH_METRICA
            WHERE nr_cpf = :cpf
              AND EXTRACT(MONTH FROM dt_registro) = :mes
              AND EXTRACT(YEAR FROM dt_registro) = :ano
        """, {"cpf": cpf, "mes": mes_int, "ano": ano_int})
        linhas = cursor.fetchall()
        colunas = [d[0].lower() for d in cursor.description] if cursor.description else []
        cursor.close()
        if not linhas:
            print(f"\n{margem}Nenhuma métrica encontrada para esse mês.")
            input("\nPressione ENTER para continuar...")
            return
        df_metrica = pd.DataFrame(linhas, columns=colunas)
    except Exception as e:
        print(f"\n{margem}Erro ao buscar métricas: {e}")
        input("\nPressione ENTER para continuar...")
        return

    df_desempenho = calcular_desempenho(df_metrica)
    if df_desempenho.empty:
        print(f"\n{margem}Nenhum dado de desempenho calculado para este mês.")
        input("\nPressione ENTER para continuar...")
        return
    colunas_numericas = df_desempenho.select_dtypes(include="number").columns
    df_desempenho[colunas_numericas] = df_desempenho[colunas_numericas].round(2)
    df_exibir = df_desempenho.rename(columns=colunas_renomear)

    grupos_relatorio_diario = [
    ("PRODUTIVIDADE", ['Produtividade', 'Foco','Tarefas concluídas', 'Tarefas em andamento', 'Tarefas pendentes', 'Tarefas concluídas no prazo', 'Tarefas concluídas com atraso']),
    ("BEM-ESTAR EMOCIONAL", ['Estresse', 'Humor', 'Energia', 'Controle do dia']),
    ("SATISFAÇÃO NO TRABALHO", ['Satisfação', 'Relação colegas', 'Reconhecimento', 'Carga trabalho']),
    ("QUALIDADE DO SONO", ['Sono (horas)', 'Sono (descanso)', 'Despertares']),
    ("BEM-ESTAR FÍSICO", ['Atividade física', 'Água', 'Intensidade da atividade']),]
    if 'CPF' in df_exibir.columns:
        df_exibir = df_exibir.drop(columns=['CPF'])

    limpa_tela()
    imprimir_tabela(df_exibir, 
                    titulo=f"Relatório Mensal - {nome} CPF: {cpf} - Referência: {mes_int}/{ano_int}", 
                    colunas_exibir=grupos_relatorio_diario)
    feedback, insights = gerar_feedback_e_insights(df_desempenho.iloc[0].to_dict())
    print(feedback)
    for insight in insights:
        print(insight)
    gerar_dataframe(df_exibir)

def relatorio_geral(conn: oracledb.Connection, mes: int = None, ano: int = None) -> None:
    """
    Gera o relatório mensal de todas as métricas para todos os colaboradores da equipe.

    Busca todos os dados de métricas da equipe para o mês e ano especificados, calcula
    o desempenho consolidado (média de cada colaborador) e, em seguida, calcula a média
    desses consolidados para gerar insights gerais sobre a saúde da equipe.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.
        mes: O mês de referência.
        ano: O ano de referência.

    Returns:
        None: Gerencia a interação com o usuário e a exibição do relatório.
    """
    limpa_tela()
    while True:
        limpa_tela()
        print(f"\n===== RELATÓRIO GERAL MENSAL =====\n")
        if mes is None:
            mes_str = input(f"\n{margem}Digite o mês (1-12): ").strip()
        else:
            mes_str = str(mes)
        if ano is None:
            ano_str = input(f"\n{margem}Digite o ano (AAAA): ").strip()
        else:
            ano_str = str(ano)

        try:
            mes_int = int(mes_str)
            ano_int = int(ano_str)
            if 1 <= mes_int <= 12:
                break
            else:
                print(f"\n{margem}Mês inválido!")
                if not perguntar_continuar("inserir mês e ano novamente"):
                    return
        except:
            print(f"\n{margem}Mês ou ano inválido!")
            if not perguntar_continuar("inserir mês e ano novamente"):
                return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT *
            FROM T_MNDSH_METRICA
            WHERE EXTRACT(MONTH FROM dt_registro) = :mes
              AND EXTRACT(YEAR FROM dt_registro) = :ano
        """, {"mes": mes_int, "ano": ano_int})

        linhas = cursor.fetchall()
        colunas = [d[0].lower() for d in cursor.description] if cursor.description else []
        cursor.close()

        if not linhas:
            print(f"\n{margem}Nenhuma métrica encontrada para esse mês.")
            input("\nPressione ENTER para continuar...")
            return

        df_metrica = pd.DataFrame(linhas, columns=colunas)

    except Exception as e:
        print(f"\n{margem}Erro ao buscar métricas: {e}")
        input("\nPressione ENTER para continuar...")
        return

    df_desempenho = calcular_desempenho(df_metrica)

    if df_desempenho.empty:
        print(f"\n{margem}Nenhum dado de desempenho calculado para esse mês.")
        input("\nPressione ENTER para continuar...")
        return

    df_exibir = df_desempenho.rename(columns=colunas_renomear)

    colunas_numericas = df_exibir.select_dtypes(include="number").columns
    for col in colunas_numericas:
        df_exibir[col] = df_exibir[col].apply(lambda x: f"{x:.2f}")

    colunas_id = ['CPF']
    grupos_metricas = [
        ("PRODUTIVIDADE", ['Produtividade', 'Foco','Tarefas concluídas', 'Tarefas em andamento', 'Tarefas pendentes', 'Tarefas concluídas no prazo', 'Tarefas concluídas com atraso']),
        ("BEM-ESTAR EMOCIONAL", ['Estresse', 'Humor', 'Energia', 'Controle do dia']),
        ("SATISFAÇÃO NO TRABALHO", ['Satisfação', 'Relação colegas', 'Reconhecimento', 'Carga trabalho']),
        ("QUALIDADE DO SONO", ['Sono (horas)', 'Sono (descanso)', 'Despertares']),
        ("BEM-ESTAR FÍSICO", ['Atividade física', 'Água', 'Intensidade da atividade']),]
    grupos_relatorio_geral = []

    for titulo, colunas_do_grupo in grupos_metricas:
        colunas_do_grupo_completo = colunas_id + colunas_do_grupo
        grupos_relatorio_geral.append((titulo, colunas_do_grupo_completo))

    limpa_tela()
    imprimir_tabela(df_exibir, 
                    titulo=f"Relatório Geral - Referência: {mes_int}/{ano_int}", 
                    colunas_exibir=grupos_relatorio_geral)
    try:
        total_concluidas = df_desempenho["tarefas_concluidas"].sum()
        total_geral = (
            df_desempenho["tarefas_concluidas"].sum() +
            df_desempenho["tarefas_andamento"].sum() +
            df_desempenho["tarefas_pendentes"].sum())

        prop_conclusao_media = total_concluidas / total_geral if total_geral > 0 else 0
    except:
        prop_conclusao_media = 0

    metricas_media = df_desempenho.drop(columns=["nr_cpf"], errors="ignore").mean().to_dict()
    metricas_media["prop_conclusao"] = prop_conclusao_media

    feedback, insights = gerar_feedback_e_insights_geral(metricas_media)
    print(feedback)
    for insight in insights:
        print(insight)
    gerar_dataframe(df_exibir)

# ===== MENU ADMINISTRADOR =====

def menu_administrador(conn: oracledb.Connection) -> None:
    """
    Implementa o menu principal e submenus para as ações administrativas.
    Esta função é o ponto de controle para operações de CRUD de Colaboradores e Tarefas,
    além de acesso aos diversos relatórios (Diário, Mensal, Geral) do sistema.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.
    """
    while True:
        escolha = menu_opcoes(
            "===== MENU ADMINISTRADOR =====\n",
            ["Colaboradores", "Tarefas", "Relatórios", "Voltar"],
            ["colaboradores", "tarefas", "relatorios", "voltar"])

        if escolha == "colaboradores":
            while True:
                limpa_tela()
                op = menu_opcoes("===== MENU COLABORADORES=====\n",
                    ["Cadastrar", "Atualizar", "Deletar", "Listar", "Voltar"],
                    ["cadastrar", "atualizar", "deletar", "listar", "voltar"])
                if op == "cadastrar":
                    limpa_tela()
                    cadastrar_colaborador(conn)
                elif op == "atualizar":
                    limpa_tela()
                    atualizar_colaborador(conn)
                elif op == "deletar":
                    limpa_tela()
                    excluir_colaborador(conn)
                elif op == "listar":
                    limpa_tela()
                    listar_colaboradores(conn)
                elif op == "voltar":
                    print(f"\n {margem} Voltando...!")
                    input("\nPressione ENTER para continuar...")
                    limpa_tela()
                    break

        elif escolha == "tarefas":
            limpa_tela()
            while True:
                op = menu_opcoes("===== MENU TAREFAS=====\n",
                    ["Cadastrar", "Atualizar", "Deletar", "Listar", "Voltar"],
                    ["cadastrar", "atualizar", "deletar", "listar", "voltar"])
                if op == "cadastrar":
                    limpa_tela()
                    adicionar_tarefa_admin(conn)
                elif op == "atualizar":
                    limpa_tela()
                    atualizar_tarefa_admin(conn)
                elif op == "deletar":
                    limpa_tela()
                    excluir_tarefa_admin(conn)
                elif op == "listar":
                    limpa_tela()
                    listar_tarefas_admin(conn)
                elif op == "voltar":
                    print(f"\n {margem} Voltando...!")
                    input("\nPressione ENTER para continuar...")
                    limpa_tela()
                    break

        elif escolha == "relatorios":
            limpa_tela()
            while True:
                limpa_tela()
                op = menu_opcoes("===== MENU RELATÓRIOS =====\n",
                    ["Relatório Diário", "Relatório Mensal", "Relatório Geral", "Voltar"],
                    ["diario", "mensal", "geral", "voltar"])
                if op == "voltar":
                    print(f"\n {margem} Voltando...!")
                    input("\nPressione ENTER para continuar...")
                    limpa_tela()
                    break

                if op in ["diario", "mensal"]:
                    limpa_tela()
                    colaborador = buscar_colaborador(
                        conn,
                        titulo_menu=f"RELATÓRIO {'DIÁRIO' if op == 'diario' else 'MENSAL'}"
                    )
                    if not colaborador:
                        continue

                    cpf_colab = colaborador["nr_cpf"]
                    nome_colab = colaborador["nm_colaborador"]

                    if op == "diario":
                        relatorio_diario(conn, cpf=cpf_colab)

                    elif op == "mensal":
                        relatorio_mensal(conn, cpf=cpf_colab, nome=nome_colab)

                elif op == "geral":
                    relatorio_geral(conn)
                    
        elif escolha == "voltar":
            print(f"\n {margem} Voltando...!")
            input("\nPressione ENTER para continuar...")
            limpa_tela()
            break

# ===== MENU COLABORADOR =====

def menu_colaborador(conn: oracledb.Connection) -> None:
    """
    Implementa o menu de funcionalidades acessíveis ao colaborador (usuário comum).

    O fluxo começa com a seleção do colaborador (simulando o login usando o buscar_colaborador()). Uma vez selecionado,
    o usuário tem acesso às funcionalidades essenciais: gestão de tarefas pessoais,
    registro de métricas e visualização de seus relatórios.

    Args:
        conn: O objeto de conexão ativo com o banco de dados Oracle.
    """
    while True:  
        limpa_tela()
        colaborador = buscar_colaborador(conn, titulo_menu="MENU DO COLABORADOR")
        if not colaborador:
            return 

        cpf_colaborador = colaborador["nr_cpf"]
        nome_colaborador = colaborador["nm_colaborador"]

        while True: 
            limpa_tela()

            op = menu_opcoes(f"===== MENU - {nome_colaborador} (CPF: {cpf_colaborador}) =====\n",
                ["Listar Tarefas", "Atualizar Tarefa", "Registrar Métricas Diárias", "Relatório Diário", "Relatório Mensal", "Buscar outro Colaborador", "Voltar Menu Principal"],
                ["listar", "atualizar", "registrar", "rel_diario", "rel_mensal", "buscar_outro_colaborador", "voltar_menu_principal"])

            if op == "listar":
                limpa_tela()
                listar_tarefas_colaborador(conn, cpf_colaborador)

            elif op == "atualizar":
                limpa_tela()
                atualizar_tarefa_colaborador(conn,  cpf_colaborador, nome_colaborador)

            elif op == "registrar":
                limpa_tela()
                registrar_metrica(conn, cpf_colaborador)

            elif op == "rel_diario":
                relatorio_diario(conn, cpf_colaborador)

            elif op == "rel_mensal":
                relatorio_mensal(conn, cpf=cpf_colaborador, nome=nome_colaborador)

            elif op == "buscar_outro_colaborador": 
                limpa_tela() 
                break 

            elif op == "voltar_menu_principal":
                print(f"\n {margem} Voltando ao Menu Principal...!")
                input("\nPressione ENTER para continuar...")
                limpa_tela()
                return 
