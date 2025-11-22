import biblioteca as _b

margem = ' ' * 4

conn = _b.conectarBD()

while True:
    escolha = _b.menu_opcoes(
        "===== MENU PRINCIPAL =====\n",
        ["Administrador", "Colaborador", "Sair"],
        ["administrador", "colaborador", "sair"]
    )
    if escolha == "administrador":
        _b.limpa_tela()
        _b.menu_administrador(conn)
    elif escolha == "colaborador":
        _b.limpa_tela()
        _b.menu_colaborador(conn)
    elif escolha == "sair":
        print(f"\n {margem} Encerrando sistema...\n")
        break

conn.close()


