from flask import Flask, render_template
import json

app = Flask(__name__)

@app.route('/')
def index():
    # Lê o arquivo JSON
    with open('estacoes_retorno.json') as file:
        data = json.load(file)

    # Renderiza o template HTML passando o JSON como contexto
    return render_template('aeroporto.html', data=data)

if __name__ == '__main__':
    app.run()