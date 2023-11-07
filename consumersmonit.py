import requests
import sys
import warnings
import configparser

warnings.filterwarnings("ignore")
config = configparser.ConfigParser()
config.read('kafkaplugin.ini')
url = config['API']['Endpoint']


def get_consumer_groups(url):
    
    response = requests.get(url,verify=False)

    if response.status_code == 200:
        return response.json()["consumerGroups"]
    else:
        print(f"Error al hacer la solicitud. Código de respuesta: {response.status_code}")
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
