import asyncio
import socket
import struct
import sys
from collections import deque
from datetime import datetime
import ipaddress

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


async def get_port_from_user(port_type, default_port):
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


async def main(args):
    global local_name, local_ip, udp_client, is_running, udp_port, tcp_port
    if len(args) < 3:
        print("Использование: python p2p_chat.py <имя> <ip>")
        return
    local_name = args[1]

    try:
        ipaddress.ip_address(args[2])
        local_ip = args[2]
    except ValueError:
        print("Ошибка: Неверный формат IP-адреса")
        return

    udp_port = await get_port_from_user('UDP', DEFAULT_UDP_PORT)
    tcp_port = await get_port_from_user('TCP', DEFAULT_TCP_PORT)
    print(f"\nЗапуск {local_name} на {local_ip} (UDP:{udp_port}, TCP:{tcp_port})")

    udp_client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_client.bind((local_ip, udp_port))

    try:
        tasks = [
            asyncio.create_task(send_udp_broadcasts()),
            asyncio.create_task(start_udp_listener()),
            asyncio.create_task(start_tcp_listener()),
            asyncio.create_task(handle_user_input())
        ]
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nЗавершение работы...")
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
    finally:
        is_running = False
        await shutdown_cleanup()


async def shutdown_cleanup():
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

    for addr, (sock, _) in list(tcp_clients.items()):
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


async def send_udp_broadcasts():
    await asyncio.sleep(0.1)
    message = create_message(MessageTypes.UserEntered, local_name)
    broad_address = ('255.255.255.255', udp_port)
    try:
        udp_client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_client.sendto(message, broad_address)
        print(f"Широковещательное сообщение отправлено на {broad_address}")
    except Exception as ex:
        print(f"Ошибка отправки широковещательного сообщения: {ex}")


async def start_udp_listener():
    print(f"Прослушивание UDP соединений на {local_ip}:{udp_port}")
    while is_running:
        try:
            data, addr = await asyncio.get_event_loop().sock_recvfrom(udp_client, 1024)
            ip = addr[0]
            if ip != local_ip:
                print(f"Получено UDP от {addr} | {datetime.now().strftime('%H:%M:%S')}")
                await process_udp_message(data, ip)
        except asyncio.CancelledError:
            break
        except OSError as e:
            if not is_running:
                break
            print(f"Ошибка UDP прослушивания: {e}")
            break
        except Exception as ex:
            print(f"Неожиданная ошибка в UDP прослушивании: {ex}")
            break


async def process_udp_message(data, ip):
    try:
        message_type = data[0]
        message_length = struct.unpack('!H', data[1:3])[0]
        username = data[3:3 + message_length].decode('utf-8')

        if message_type == MessageTypes.UserEntered:
            for addr in list(tcp_clients.keys()):
                if addr[0] == ip:
                    return

            await create_tcp_connection(ip, username)
    except Exception as e:
        print(f"Ошибка обработки UDP сообщения: {e}")


async def start_tcp_listener():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((local_ip, tcp_port))
    server.listen()
    server.setblocking(False)
    print(f"Прослушивание TCP соединений на {local_ip}:{tcp_port}")

    while is_running:
        try:
            client_socket, addr = await asyncio.get_event_loop().sock_accept(server)
            print(f"Принято соединение {addr} | {datetime.now().strftime('%H:%M:%S')}")

            try:
                data = await asyncio.get_event_loop().sock_recv(client_socket, 1024)
                message_type = data[0]
                message_length = struct.unpack('!H', data[1:3])[0]
                username = data[3:3 + message_length].decode('utf-8')

                if message_type != MessageTypes.UserEntered:
                    client_socket.close()
                    continue

                tcp_clients[addr] = (client_socket, username)

                await send_chat_history(client_socket)

                new_user_msg = f"{username} присоединился к чату  | {datetime.now().strftime('%H:%M:%S')}"
                print(new_user_msg)
                history.append(f"{new_user_msg} | {datetime.now().strftime('%H:%M:%S')}")
                message = create_message(MessageTypes.UserEntered, new_user_msg)

                for client_addr, (sock, _) in list(tcp_clients.items()):
                    if client_addr != addr:
                        try:
                            await asyncio.get_event_loop().sock_sendall(sock, message)
                        except Exception as e:
                            print(f"Ошибка отправки уведомления на {client_addr}: {e}")
                            sock.close()
                            if client_addr in tcp_clients:
                                del tcp_clients[client_addr]

                asyncio.create_task(receive_tcp_message(client_socket, addr, username))
            except Exception as e:
                print(f"Ошибка получения имени пользователя: {e}")
                client_socket.close()

        except asyncio.CancelledError:
            break
        except Exception as e:
            if is_running:
                print(f"Ошибка TCP прослушивания: {e}")
            break
    server.close()


async def handle_user_input():
    loop = asyncio.get_event_loop()

    while is_running:
        try:
            message = await loop.run_in_executor(None, input)

            if message.lower() == "/exit":
                await user_exit()
                return

            current_time = datetime.now().strftime("%H:%M:%S")
            formatted_message = f"{local_name} ({local_ip}): {message} | {current_time}"
            message_bytes = create_message(MessageTypes.Message, formatted_message)

            if formatted_message not in history:
                history.append(formatted_message)

                for addr, (sock, _) in list(tcp_clients.items()):
                    try:
                        await loop.sock_sendall(sock, message_bytes)
                    except Exception as e:
                        print(f"Ошибка отправки на {addr}: {e}")
                        sock.close()
                        if addr in tcp_clients:
                            del tcp_clients[addr]
                        await notify_user_left(addr)

        except (KeyboardInterrupt, EOFError):
            await user_exit()
            return
        except Exception as e:
            print(f"Неожиданная ошибка: {e}")


async def user_exit():
    global is_running
    is_running = False
    print("\nВыход из чата...")

    message = create_message(MessageTypes.UserLeft,
                             f"{local_name} вышел из чата. | {datetime.now().strftime('%H:%M:%S')}")

    for addr, (sock, _) in list(tcp_clients.items()):
        try:
            if not sock._closed:
                await asyncio.get_event_loop().sock_sendall(sock, message)
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


async def receive_tcp_message(client_socket, addr, username):
    loop = asyncio.get_event_loop()
    disconnected_msg_printed = False

    try:
        while is_running:
            try:
                data = await loop.sock_recv(client_socket, 1024)
                if not data:
                    if not disconnected_msg_printed:
                        print(f"Клиент {username} ({addr[0]}) отключился. | {datetime.now().strftime('%H:%M:%S')}")
                        disconnected_msg_printed = True
                    break
                await process_message(data, addr, username)
            except ConnectionResetError:
                if not disconnected_msg_printed:
                    print(f"Клиент {username} ({addr[0]}) отключился. | {datetime.now().strftime('%H:%M:%S')}")
                    disconnected_msg_printed = True
                break
            except asyncio.CancelledError:
                break
            except Exception:
                if not disconnected_msg_printed:
                    disconnected_msg_printed = True
                break
    finally:
        try:
            if addr in tcp_clients:
                del tcp_clients[addr]
            client_socket.close()
            if not disconnected_msg_printed:
                await notify_user_left(addr, username)
        except Exception as e:
            if not disconnected_msg_printed:
                print(f"Ошибка очистки для {addr}: {e}")


async def process_message(data, addr, username):
    try:
        message_type = data[0]
        message_length = struct.unpack('!H', data[1:3])[0]
        content = data[3:3 + message_length].decode('utf-8')

        if addr[0] == local_ip:
            return

        if message_type == MessageTypes.Message:
            if content == "END_OF_HISTORY":
                return
            if content not in history:
                print(content)
                history.append(content)
        elif message_type == MessageTypes.UserLeft:
            if content not in history:
                print(content)
                history.append(content)
    except Exception as e:
        print(f"Ошибка обработки сообщения: {e}")


async def create_tcp_connection(ip, username):
    if ip == local_ip:
        return

    target_end_point = (ip, tcp_port)

    for addr in list(tcp_clients.keys()):
        if addr[0] == ip:
            print(f"Уже подключен к {username} ({addr})")
            return

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.setblocking(False)
        client_socket.bind((local_ip, 0))

        try:
            await asyncio.get_event_loop().sock_connect(client_socket, target_end_point)
        except OSError as e:
            print(f"Ошибка подключения к {target_end_point}: {e}")
            client_socket.close()
            return

        print(f"{username} присоединился к чату | {datetime.now().strftime('%H:%M:%S')}")

        enter_msg = local_name
        message = create_message(MessageTypes.UserEntered, enter_msg)
        await asyncio.get_event_loop().sock_sendall(client_socket, message)

        tcp_clients[target_end_point] = (client_socket, username)
        await send_chat_history(client_socket)

        asyncio.create_task(receive_tcp_message(client_socket, target_end_point, username))
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        if target_end_point in tcp_clients:
            del tcp_clients[target_end_point]
        client_socket.close()


async def send_chat_history(client_socket):
    try:
        history_copy = list(history)
        for msg in history_copy:

            if "присоединился к чату" not in msg:
                message = create_message(MessageTypes.Message, msg)
                await asyncio.get_event_loop().sock_sendall(client_socket, message)
                await asyncio.sleep(0.01)

        end_message = create_message(MessageTypes.Message, "END_OF_HISTORY")
        await asyncio.get_event_loop().sock_sendall(client_socket, end_message)
    except Exception as e:
        print(f"Ошибка отправки истории: {e}")


def create_message(message_type, content):
    content_encoded = content.encode()
    message_length = len(content_encoded)
    return struct.pack('!B H', message_type, message_length) + content_encoded


async def notify_user_left(addr, username=None):
    try:
        if addr[0] == local_ip:
            return

        username = username or addr[0]
        user_left_message = create_message(
            MessageTypes.UserLeft,
            f"Пользователь {username} ({addr[0]}) вышел из чата. | {datetime.now().strftime('%H:%M:%S')}"
        )

        for client_addr, (client_socket, _) in list(tcp_clients.items()):
            if client_addr != addr:
                try:
                    await asyncio.get_event_loop().sock_sendall(client_socket, user_left_message)
                except Exception as e:
                    print(f"Ошибка уведомления {client_addr}: {e}")
                    client_socket.close()
                    if client_addr in tcp_clients:
                        del tcp_clients[client_addr]
    except Exception as e:
        print(f"Ошибка в notify_user_left: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main(sys.argv))
    except KeyboardInterrupt:
        pass