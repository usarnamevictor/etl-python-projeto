import pandas as pd

# =========================
# EXTRACT
# =========================
def extract_data(path):
    print("Extraindo dados...")
    df = pd.read_csv(path)
    return df


# =========================
# TRANSFORM
# =========================
def generate_message(nome, cartao):
    return f"""
Olá, {nome}!

Parabéns! Seu cartão {cartao} possui benefícios exclusivos.
Continue utilizando nossos serviços para aproveitar vantagens especiais!

Atenciosamente,
Equipe Santander Dev Week
""".strip()


def transform_data(df):
    print("Transformando dados...")
    df["mensagem"] = df.apply(
        lambda row: generate_message(row["nome"], row["cartao"]),
        axis=1
    )
    return df


# =========================
# LOAD
# =========================
def load_data(df, output_path):
    print("Carregando dados...")
    df.to_csv(output_path, index=False)
    print("Arquivo salvo com sucesso!")


# =========================
# PIPELINE ETL
# =========================
if __name__ == "__main__":
    input_file = "../data/usuarios.csv"
    output_file = "../data/mensagens_geradas.csv"

    dados = extract_data(input_file)
    dados_transformados = transform_data(dados)
    load_data(dados_transformados, output_file)
