#!/usr/bin/env python3
"""HNK LST - simple matchmaking coordinator for IKEMEN GO 1v1.

stdlib-only HTTP service. Not a game relay: peers still connect with IKEMEN GO.
To run behind Render set PORT and TRUST_PROXY=1 (only behind a trusted reverse proxy).
"""
from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import secrets
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

BASE_DIR = Path(__file__).resolve().parent
USERNAME_RE = re.compile(r"^[A-Za-z0-9._-]{3,20}$")
VERSION_RE = re.compile(r"^[A-Za-z0-9 ._+-]{1,60}$")
HOST_RE = re.compile(r"^[A-Za-z0-9.-]{1,190}$")
WAIT_TTL = 100
MATCH_TTL = 360
MAX_BODY = 3072
JOIN_LIMIT_PER_MINUTE = 15


def valid_address(raw):
    if not isinstance(raw, str):
        return False
    if len(raw) > 190 or not raw:
        return False
    try:
        ipaddress.ip_address(raw)
        return True
    except ValueError:
        return bool(HOST_RE.fullmatch(raw)) and '..' not in raw and not raw.startswith('-') and not raw.endswith('-')


def error(message, code=400):
    return {'ok': False, 'error': message}, code


@dataclass
class Player:
    token: str
    nickname: str
    game: str
    friend: str
    can_host: bool
    ip: str
    last_seen: float
    match_id: str = ''


class Matchmaker:
    def __init__(self):
        self.lock = threading.RLock()
        self.players: dict[str, Player] = {}
        self.names: dict[str, str] = {}
        self.waiting: list[str] = []
        self.matches: dict[str, dict] = {}
        self.attempts: dict[str, deque[float]] = defaultdict(deque)

    def _drop(self, token):
        p = self.players.pop(token, None)
        if not p:
            return False
        if self.names.get(p.nickname.casefold()) == token:
            self.names.pop(p.nickname.casefold(), None)
        if token in self.waiting:
            self.waiting.remove(token)
        if p.match_id in self.matches:
            m = self.matches[p.match_id]
            m['closed'] = True
        return True

    def _clean(self):
        now = time.monotonic()
        for token, p in list(self.players.items()):
            if now - p.last_seen > (MATCH_TTL if p.match_id else WAIT_TTL):
                self._drop(token)
        active = {p.match_id for p in self.players.values() if p.match_id}
        for mid in list(self.matches):
            if mid not in active:
                del self.matches[mid]
        for remote, times in list(self.attempts.items()):
            while times and now - times[0] > 60:
                times.popleft()
            if not times:
                del self.attempts[remote]

    def _compatible(self, player, other):
        if player.game != other.game or not (player.can_host or other.can_host):
            return False
        if bool(player.friend) != bool(other.friend):
            return False
        if player.friend:
            return (player.friend.casefold() == other.nickname.casefold()
                    and other.friend.casefold() == player.nickname.casefold())
        return True

    def join(self, body, remote_ip):
        nickname = body.get('username', body.get('nickname', ''))
        friend = body.get('friend', body.get('friend_username', body.get('target_username', '')))
        game = body.get('game', body.get('version', 'HNK LST'))
        address = body.get('host_address', remote_ip)
        can_host = body.get('can_host', True)
        if not isinstance(nickname, str) or not USERNAME_RE.fullmatch(nickname):
            return error('Username non valido: 3-20 caratteri (lettere, numeri, . _ -)')
        if not isinstance(friend, str) or (friend and not USERNAME_RE.fullmatch(friend)):
            return error('Username amico non valido (3-20 caratteri)')
        if friend and friend.casefold() == nickname.casefold():
            return error('Non puoi cercare te stesso')
        if not isinstance(game, str) or not VERSION_RE.fullmatch(game):
            return error('Versione del gioco non valida')
        if not isinstance(can_host, bool):
            return error('can_host deve essere true o false')
        if not valid_address(address):
            return error('Indirizzo host non valido')
        with self.lock:
            self._clean()
            history = self.attempts[remote_ip]
            if len(history) >= JOIN_LIMIT_PER_MINUTE:
                return error('Troppi tentativi: riprova tra un minuto', 429)
            history.append(time.monotonic())
            if nickname.casefold() in self.names:
                return error('Username già in uso, scegli un altro nome', 409)
            token = secrets.token_urlsafe(32)
            player = Player(token, nickname, game, friend, can_host, address, time.monotonic())
            self.players[token] = player
            self.names[nickname.casefold()] = token
            for other_token in list(self.waiting):
                other = self.players.get(other_token)
                if other and self._compatible(player, other):
                    host = other if other.can_host else player
                    guest = player if host is other else other
                    mid = secrets.token_urlsafe(16)
                    self.matches[mid] = {'host': host.token, 'guest': guest.token, 'closed': False}
                    other.match_id = player.match_id = mid
                    self.waiting.remove(other_token)
                    # Nomi liberati al momento dell'abbinamento; le sessioni restano
                    # raggiungibili con token per l'avvio della connessione.
                    self.names.pop(player.nickname.casefold(), None)
                    self.names.pop(other.nickname.casefold(), None)
                    break
            if not player.match_id:
                self.waiting.append(token)
            return {'ok': True, 'token': token, 'status': 'matched' if player.match_id else 'waiting'}, 200

    def status(self, token):
        if not isinstance(token, str) or len(token) > 150:
            return error('Token non valido')
        with self.lock:
            self._clean()
            p = self.players.get(token)
            if not p:
                return {'ok': True, 'status': 'expired', 'message': 'Ricerca scaduta: riprova'}, 200
            p.last_seen = time.monotonic()
            data = {'ok': True, 'username': p.nickname, 'status': 'waiting',
                    'online': len(self.players), 'in_queue': len(self.waiting),
                    'poll_after_ms': 2000}
            if not p.match_id:
                data['message'] = ('In attesa che il tuo amico ti cerchi' if p.friend
                                   else 'Ricerca avversario in corso')
                return data, 200
            m = self.matches.get(p.match_id)
            if not m or m['closed']:
                data.update(status='opponent_left', message='Avversario disconnesso')
                return data, 200
            is_host = m['host'] == token
            opp = self.players.get(m['guest'] if is_host else m['host'])
            host = self.players.get(m['host'])
            if not opp or not host:
                data.update(status='opponent_left', message='Avversario disconnesso')
                return data, 200
            data.update(status='matched', match_id=p.match_id,
                        role='host' if is_host else 'guest',
                        opponent=opp.nickname, host_address=host.ip,
                        port=7500, message='Avversario trovato')
            return data, 200

    def leave(self, token):
        with self.lock:
            self._clean()
            existed = self._drop(token)
            return {'ok': True, 'removed': existed}, 200

    def health(self):
        with self.lock:
            self._clean()
            return {'ok': True, 'service': 'HNK LST Matchmaker',
                    'online': len(self.players), 'in_queue': len(self.waiting)}, 200


STATE = Matchmaker()


class Handler(BaseHTTPRequestHandler):
    server_version = 'HNKLSTMatchmaker/2.0'

    def log_message(self, fmt, *args):
        # Suppress token-bearing HTTP access logs (GET status puts token in URL).
        pass

    def remote_ip(self):
        address = self.client_address[0]
        if os.getenv('TRUST_PROXY', '').lower() in ('1', 'true', 'yes'):
            forwarded = self.headers.get('X-Forwarded-For', '')
            # Only use the forwarding header when behind a controlled proxy.
            if forwarded:
                candidate = forwarded.split(',')[-1].strip()
                try:
                    ipaddress.ip_address(candidate)
                    return candidate
                except ValueError:
                    pass
        return address

    def send_json(self, obj, code=200):
        data = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Max-Age', '600')
        self.end_headers()

    def do_GET(self):
        u = urlsplit(self.path)
        if u.path in ('/api/health', '/health'):
            response, code = STATE.health()
            return self.send_json(response, code)
        if u.path == '/api/status':
            token = parse_qs(u.query).get('token', [''])[0]
            response, code = STATE.status(token)
            return self.send_json(response, code)
        if u.path in ('/', '/index.html', '/app.js'):
            content = (BASE_DIR / ('app.js' if u.path == '/app.js' else 'index.html')).read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', 'application/javascript; charset=utf-8' if u.path == '/app.js' else 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'")
            self.end_headers()
            return self.wfile.write(content)
        self.send_json(*error('Pagina non trovata', 404))

    def do_POST(self):
        path = urlsplit(self.path).path
        if path not in ('/api/join', '/api/search', '/api/leave', '/api/cancel'):
            return self.send_json(*error('Endpoint non trovato', 404))
        try:
            size = int(self.headers.get('Content-Length', '0'))
        except ValueError:
            return self.send_json(*error('Lunghezza errata'))
        if size < 2 or size > MAX_BODY:
            return self.send_json(*error('JSON mancante o troppo grande', 413))
        try:
            obj = json.loads(self.rfile.read(size))
            if not isinstance(obj, dict):
                raise ValueError()
        except (ValueError, UnicodeDecodeError):
            return self.send_json(*error('JSON non valido'))
        if path in ('/api/leave', '/api/cancel'):
            response, code = STATE.leave(obj.get('token', ''))
        else:
            response, code = STATE.join(obj, self.remote_ip())
        self.send_json(response, code)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=int(os.environ.get('PORT', '8080')))
    a = parser.parse_args()
    server = ThreadingHTTPServer((a.host, a.port), Handler)
    print(f'HNK LST Matchmaker v2.0 http://{a.host}:{server.server_port} - CTRL+C per fermare', flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
