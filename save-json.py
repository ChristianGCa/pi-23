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

            fields = {
                'deviceName': estacao['deviceInfo']['deviceName'],
                'temperature_internal': estacao['object']['internal_sensors'][0]['v'], #Unidade = C
                'humidity_internal': estacao['object']['internal_sensors'][1]['v'], #Unidade = %
                'rain_level': estacao['object']['modules'][0]['v'], #Unidade = mm
                'avg_wind_speed': estacao['object']['modules'][1]['v'], #Unidade = km/h
                'gust_wind_speed': estacao['object']['modules'][2]['v'], #Unidade = km/h
                'wind_direction': estacao['object']['modules'][3]['v'], #Unidade = graus
                'temperature': estacao['object']['modules'][4]['v'], #Unidade = C
                'humidity': estacao['object']['modules'][5]['v'], #Unidade = %
                'luminosity': estacao['object']['modules'][6]['v'], #Unidade = lx
                'uv': estacao['object']['modules'][7]['v'], #Unidade = /
                'solar_radiation': estacao['object']['modules'][8]['v'], #Unidade = W/m²
                'atm_pres': estacao['object']['modules'][9]['v'] #Unidade = hPa
            }

            object_json = json.dumps(fields, ensure_ascii=False, indent=4)

            if contador == 0:
                arq.write(object_json +"," + '\n' + '\n')
                contador = contador + 1
            else:
                arq.write(object_json + '\n' + '\n' + "]")
            
        time.sleep(2)
        i += 1

except KeyboardInterrupt:
    print('\nSaindo...')