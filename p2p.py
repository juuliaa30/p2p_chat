import socket
import struct
import sys
from collections import deque
from datetime import datetime
import threading
import time
import select

DEFAULT_UDP_PORT = 2222
DEFAULT_TCP_PORT = 5555

local_name = None
local_ip = None
udp_port = DEFAULT_UDP_PORT
tcp_port = DEFAULT_TCP_PORT
udp_client = None
history = deque()
tcp_clients = {}
is_running = True
lock = threading.Lock()


class MessageTypes:
    Message = 1
    History = 2
    UserEntered = 3
    UserLeft = 4


def is_port_available(port, protocol='tcp'):
    sock_type = socket.SOCK_STREAM if protocol == 'tcp' else socket.SOCK_DGRAM
    try:
        with socket.socket(socket.AF_INET, sock_type) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('127.0.0.1', port))
            if protocol == 'tcp':
                s.listen(1)
            return True
    except PermissionError:
        return False
    except OSError as e:
        if e.errno == 10013:
            print(f"Порт {port} заблокирован. Попробуйте другой порт.")
        elif e.errno == 98:
            print(f"Порт {port} уже используется.")
        return False
    except Exception:
        return False


def get_port_from_user(port_type, default_port):
    while True:
        try:
            port_str = input(f"Введите {port_type} порт (по умолчанию {default_port}): ").strip()
            if not port_str:
                return default_port
            port = int(port_str)
            if not is_port_available(port, port_type.lower()):
                continue
            return port
        except ValueError:
            print("Пожалуйста, введите корректный номер порта")


def main(args):
    global local_name, local_ip, udp_client, is_running, udp_port, tcp_port

    if len(args) < 3:
        print("Использование: python p2p_chat.py <имя> <ip>")
        return

    local_name = args[1]
    local_ip = args[2]
    udp_port = get_port_from_user('UDP', DEFAULT_UDP_PORT)
    tcp_port = get_port_from_user('TCP', DEFAULT_TCP_PORT)

    print(f"\nЗапуск {local_name} на {local_ip} (UDP:{udp_port}, TCP:{tcp_port})")

    # Инициализация UDP клиента
    udp_client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_client.bind((local_ip, udp_port))

    # Запуск потоков
    threads = [
        threading.Thread(target=send_udp_broadcasts),
        threading.Thread(target=start_udp_listener),
        threading.Thread(target=start_tcp_listener),
        threading.Thread(target=handle_user_input)
    ]

    for t in threads:
        t.daemon = True
        t.start()

    try:
        while is_running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nЗавершение работы...")
    finally:
        is_running = False
        shutdown_cleanup()

        # Ожидание завершения потоков
        for t in threads:
            if t.is_alive():
                t.join(timeout=1)


def shutdown_cleanup():
    global udp_client
    print("Очистка ресурсов...")

    if udp_client and is_running:
        try:
            message = create_message(MessageTypes.UserLeft,
                                     f"{local_name} вышел из чата. | {datetime.now().strftime('%H:%M:%S')}")
            broad_address = ('255.255.255.255', udp_port)
            udp_client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            udp_client.sendto(message, broad_address)
        except:
            pass

    with lock:
        for addr, sock in list(tcp_clients.items()):
            try:
                sock.close()
            except:
                pass
            del tcp_clients[addr]

    if udp_client:
        try:
            udp_client.close()
        except:
            pass
        udp_client = None


def send_udp_broadcasts():
    time.sleep(0.1)
    message = create_message(MessageTypes.UserEntered, local_name)
    broad_address = ('255.255.255.255', udp_port)

    try:
        udp_client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_client.sendto(message, broad_address)
        print(f"Широковещательное сообщение отправлено на {broad_address}")
    except Exception as ex:
        print(f"Ошибка отправки широковещательного сообщения: {ex}")


def start_udp_listener():
    print(f"Прослушивание UDP соединений на {local_ip}:{udp_port}")

    while is_running:
        try:
            ready = select.select([udp_client], [], [], 1)
            if ready[0]:
                data, addr = udp_client.recvfrom(1024)
                print(f"Получено UDP от {addr} | {datetime.now().strftime('%H:%M:%S')}")
                create_tcp_connection(addr[0])
        except Exception as ex:
            if not is_running:
                break
            print(f"Ошибка UDP прослушивания: {ex}")
            break


def start_tcp_listener():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((local_ip, tcp_port))
    server.listen()
    server.settimeout(1)

    print(f"Прослушивание TCP соединений на {local_ip}:{tcp_port}")

    while is_running:
        try:
            client_socket, addr = server.accept()
            print(f"Принято соединение от {addr} | {datetime.now().strftime('%H:%M:%S')}")

            with lock:
                tcp_clients[addr] = client_socket

            threading.Thread(target=receive_tcp_message, args=(client_socket, addr)).start()
        except socket.timeout:
            continue
        except Exception as e:
            if not is_running:
                break
            print(f"Ошибка TCP прослушивания: {e}")
            break

    server.close()


def handle_user_input():
    while is_running:
        try:
            message = input()
            if message.lower() == "/exit":
                user_exit()
                return

            current_time = datetime.now().strftime("%H:%M:%S")
            formatted_message = f"{local_name} ({local_ip}): {message} | {current_time}"
            message_bytes = create_message(MessageTypes.Message, formatted_message)

            history.append(formatted_message)

            with lock:
                for addr, sock in list(tcp_clients.items()):
                    try:
                        sock.sendall(message_bytes)
                    except Exception as e:
                        print(f"Ошибка отправки на {addr}: {e}")
                        sock.close()
                        if addr in tcp_clients:
                            del tcp_clients[addr]
                        notify_user_left(addr)
        except (KeyboardInterrupt, EOFError):
            user_exit()
            return
        except Exception as e:
            print(f"Неожиданная ошибка: {e}")


def user_exit():
    global is_running
    is_running = False
    print("\nВыход из чата...")

    message = create_message(MessageTypes.UserLeft,
                             f"{local_name} вышел из чата. | {datetime.now().strftime('%H:%M:%S')}")

    with lock:
        for addr, sock in list(tcp_clients.items()):
            try:
                if not sock._closed:
                    sock.sendall(message)
            except:
                pass
            finally:
                try:
                    sock.close()
                except:
                    pass
                if addr in tcp_clients:
                    del tcp_clients[addr]

    global udp_client
    if udp_client:
        try:
            udp_client.close()
        except:
            pass
        udp_client = None


def receive_tcp_message(client_socket, addr):
    disconnected_msg_printed = False

    try:
        while is_running:
            try:
                ready = select.select([client_socket], [], [], 1)
                if ready[0]:
                    data = client_socket.recv(1024)
                    if not data:
                        if not disconnected_msg_printed:
                            print(f"Клиент {addr} отключился. | {datetime.now().strftime('%H:%M:%S')}")
                            disconnected_msg_printed = True
                        break

                    process_message(data, addr)
            except ConnectionResetError:
                if not disconnected_msg_printed:
                    print(f"Клиент {addr} принудительно отключен")
                    disconnected_msg_printed = True
                break
            except Exception:
                if not disconnected_msg_printed:
                    print("Вы отключены")
                    disconnected_msg_printed = True
                break
    finally:
        with lock:
            if addr in tcp_clients:
                del tcp_clients[addr]

        try:
            client_socket.close()
        except:
            pass

        if not disconnected_msg_printed:
            notify_user_left(addr)


def process_message(data, addr):
    try:
        message_type = data[0]
        message_length = struct.unpack('!H', data[1:3])[0]
        content = data[3:3 + message_length].decode()

        if message_type == MessageTypes.Message:
            if content == "END_OF_HISTORY":
                return

            if content not in history:
                print(content)
                history.append(content)

                with lock:
                    for client_addr, client_socket in list(tcp_clients.items()):
                        if client_addr != addr:
                            try:
                                client_socket.sendall(data)
                            except Exception as e:
                                print(f"Ошибка пересылки сообщения на {client_addr}: {e}")
                                client_socket.close()
                                if client_addr in tcp_clients:
                                    del tcp_clients[client_addr]
        elif message_type == MessageTypes.UserLeft:
            print(content)
    except Exception as e:
        print(f"Ошибка обработки сообщения: {e}")


def create_tcp_connection(ip):
    target_end_point = (ip, tcp_port)

    if target_end_point[0] == local_ip:
        return

    with lock:
        if target_end_point in tcp_clients:
            print(f"Уже подключен к {target_end_point}")
            return

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(5)

        try:
            client_socket.connect(target_end_point)
        except Exception as e:
            print(f"Ошибка подключения к {target_end_point}: {e}")
            client_socket.close()
            return

        with lock:
            tcp_clients[target_end_point] = client_socket

        print(f"Подключено к {target_end_point} | {datetime.now().strftime('%H:%M:%S')}")

        send_chat_history(client_socket)

        message = create_message(MessageTypes.UserEntered,
                                 f"{local_name} присоединился к чату. | {datetime.now().strftime('%H:%M:%S')}")
        client_socket.sendall(message)

        threading.Thread(target=receive_tcp_message, args=(client_socket, target_end_point)).start()
    except Exception as e:
        print(f"Ошибка подключения к {target_end_point}: {e}")
        with lock:
            if target_end_point in tcp_clients:
                del tcp_clients[target_end_point]
        client_socket.close()


def send_chat_history(client_socket):
    try:
        for msg in history:
            message = create_message(MessageTypes.Message, msg)
            client_socket.sendall(message)
            time.sleep(0.01)

        end_message = create_message(MessageTypes.Message, "END_OF_HISTORY")
        client_socket.sendall(end_message)
    except Exception as e:
        print(f"Ошибка отправки истории: {e}")


def create_message(message_type, content):
    content_encoded = content.encode()
    message_length = len(content_encoded)
    return struct.pack('!B H', message_type, message_length) + content_encoded


def notify_user_left(addr):
    try:
        if addr[0] == local_ip:
            return

        user_left_message = create_message(
            MessageTypes.UserLeft,
            f"Пользователь {addr[0]}:{addr[1]} вышел из чата. | {datetime.now().strftime('%H:%M:%S')}"
        )

        with lock:
            for client_addr, client_socket in list(tcp_clients.items()):
                if client_addr != addr:
                    try:
                        client_socket.sendall(user_left_message)
                    except Exception as e:
                        print(f"Ошибка уведомления {client_addr}: {e}")
                        client_socket.close()
                        if client_addr in tcp_clients:
                            del tcp_clients[client_addr]
    except Exception as e:
        print(f"Ошибка в notify_user_left: {e}")


if __name__ == "__main__":
    try:
        main(sys.argv)
    except KeyboardInterrupt:
        pass