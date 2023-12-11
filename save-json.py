from rethinkdb import RethinkDB
import time
import os
import json

# Criando um objeto r para usar as funções do rethink
r = RethinkDB()

# Conectando no banco
conn = r.connect(
    # ip interno: 10.41.20.16
    # ip externo: 200.17.86.19
    host='200.17.86.19',
    port=58015,
    db='santa_rosa',
    user='eduardo.rodrigues@sou.unijui.edu.br',
    password='f37jnf9b87cf3'
)
i = 0

try:
    # Laço para mandar o select para o banco de dados
    while True:
        # Abrindo o arquivo de json para armazenar o retorno do banco de dados
        arq = open("estacoes_retorno.json", "w", encoding="utf-8")
        arq.write("[")

        # Limpando o terminal
        os.system('clear')

        # Imprimindo o cabeçalho
        print("///////////////////////////// IoTempo /////////////////////////////")
        print("\nRodando...")
        print("\nDados salvos no arquivo: estacoes_retorno.json")
        print("Atualizações: ", i, "\n")
        print("CTRL + C para sair\n")

        # Obtendo os dados do banco de dados
        data = r.db('santa_rosa').table('estacoes_metereologicas').run(conn)

        contador = 0

        # Para cada estacao (Cruzeiro e aeroporto), criar um dicionário com os itens de cada um
        for estacao in data:
            try:
                fields = {
                    'deviceName': estacao['deviceInfo']['deviceName'],
                    'temperature_internal': estacao['object']['internal_sensors'][0]['v'],
                    'humidity_internal': estacao['object']['internal_sensors'][1]['v'],
                    'rain_level': estacao['object']['modules'][0]['v'],
                    'avg_wind_speed': estacao['object']['modules'][1]['v'],
                    'gust_wind_speed': estacao['object']['modules'][2]['v'],
                    'wind_direction': estacao['object']['modules'][3]['v'],
                    'temperature': estacao['object']['modules'][4]['v'],
                    'humidity': estacao['object']['modules'][5]['v'],
                    'luminosity': estacao['object']['modules'][6]['v'],
                    'uv': estacao['object']['modules'][7]['v'],
                    'solar_radiation': estacao['object']['modules'][8]['v'],
                    'atm_pres': estacao['object']['modules'][9]['v']
                }

                object_json = json.dumps(fields, ensure_ascii=False, indent=4)

                if contador == 0:
                    arq.write(object_json + "," + '\n' + '\n')
                    contador = contador + 1
                else:
                    arq.write(object_json + '\n' + '\n' + "]")

                

            except (KeyError, IndexError) as e:
                # Lidar com exceções específicas (KeyError e IndexError) aqui
                print(f"Erro: {e}. Pulando para a próxima iteração.")
        i += 1
        time.sleep(5)

except KeyboardInterrupt:
    print('\nSaindo...')
