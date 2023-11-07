import requests
import sys
import warnings
import json
import configparser
import logging

logging.basicConfig(filename='consulta.log', level=logging.INFO)
file_handler = logging.FileHandler('consulta.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger('').addHandler(file_handler)

warnings.filterwarnings("ignore")
config = configparser.ConfigParser()
config.read('kafkaplugin.ini')
url = config['API']['Endpoint']
use_mocks = config['CONFIG'].getboolean('MOCKS')

def get_consumer_groups_mock():
    try:
        with open("mock.json", "r") as mock_file:
            mock_data = json.load(mock_file)
            logging.info("Usando datos de mock.json")
            return mock_data["consumerGroups"]
    except Exception as e:
        logging.exception("Error al cargar datos de mock.json:")
        return []

def get_consumer_groups(url):
    if use_mocks:
        return get_consumer_groups_mock()
    try:
        response = requests.get(url, verify=False)

        if response.status_code == 200:
            logging.info(response.json()["consumerGroups"])
            return response.json()["consumerGroups"]
        else:
            logging.error(f"Error al hacer la solicitud. Código de respuesta: {response.status_code}")
            return []
    except Exception as e:
        logging.exception("Error al hacer la solicitud:")
        return []

def check_stability(groups):
    unstable_groups = []

    for group in groups:
        if group["state"] != "STABLE":
            unstable_groups.append(group["groupId"])

    if unstable_groups:
        print("Los siguientes grupos de consumidores no están en estado 'STABLE':")
        for group_id in unstable_groups:
            print(group_id)
    else:
        print("Todos los grupos de consumidores están en estado 'STABLE'.")

def check_state_by_group_id(groups, group_id):
    for group in groups:
        if group["groupId"] == group_id:
            state = group["state"]
            if state == "STABLE":
                print("1")
            else:
                print("0")
            return

    print("0")

    print(f"No se encontró el grupo con ID {group_id}.")

def check_messages_behind(groups, group_id):
    for group in groups:
        if group["groupId"] == group_id:
            messages_behind = group["messagesBehind"]
            print(f" {messages_behind}")
            return

if __name__ == "__main__":
    logging.info("Ejecutando script...")
    if len(sys.argv) < 2:
        print("Debe proporcionar al menos un argumento.")
        sys.exit(1)

    arg1 = sys.argv[1]

    if arg1 == "-state":
        if len(sys.argv) == 3 and sys.argv[2] == "all":
            consumer_groups = get_consumer_groups(url)
            check_stability(consumer_groups)
        elif len(sys.argv) == 3:
            group_id = sys.argv[2]
            consumer_groups = get_consumer_groups(url)
            check_state_by_group_id(consumer_groups, group_id)
        else:
            print("Argumentos incorrectos. Use '-state all' para verificar todos los grupos o '-state <groupId>' para verificar un grupo específico.")
    elif arg1 == "-lag":
        if len(sys.argv) < 3:
            print("Debe proporcionar el ID de grupo como segundo argumento.")
            sys.exit(1)
        
        group_id = sys.argv[2]
        consumer_groups = get_consumer_groups(url)
        check_messages_behind(consumer_groups, group_id)
    else:
        print("Argumento no reconocido. Use '-state' para verificar el estado o '-lag' para verificar el lag de un grupo.")
