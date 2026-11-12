#!/usr/bin/env python3
"""Локальный приём JSON из истории OpenCode. Только стандартная библиотека."""
import argparse
from collections import Counter
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
from urllib.parse import parse_qs, urlsplit

from sonar_index import api_url, build_index


class IntakeError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise IntakeError(message)


def database_path():
    # Стандартный data-каталог; проверяем существование и схему, не создаём БД.
    root = Path(os.environ.get('XDG_DATA_HOME', Path.home() / '.local/share'))
    path = root / 'opencode/opencode.db'
    require(path.is_file(), 'Локальная база OpenCode не найдена в data-каталоге; автоматический приём недоступен в этой установке.')
    return path


V2_SCHEMA = {'id', 'session_id', 'data', 'time_created'}
V1_SCHEMA = {'session': {'id', 'directory'},
             'message': {'id', 'session_id', 'data', 'time_created'},
             'part': {'id', 'message_id', 'session_id', 'data', 'time_created'}}


def table_columns(db, table):
    return {row[1] for row in db.execute('PRAGMA table_info(' + table + ')')}


def storage_kind(db):
    # OpenCode 2 пишет сессии в session_message; OpenCode 1.x — в message/part.
    # Таблицы legacy остаются в базе после перехода, но новые записи не получают.
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'session_message' in tables and V2_SCHEMA <= table_columns(db, 'session_message'):
        return 'v2'
    if all(table in tables and required <= table_columns(db, table) for table, required in V1_SCHEMA.items()):
        return 'v1'
    raise IntakeError('Формат локальной базы OpenCode изменился; автоматический приём остановлен.')


@contextmanager
def connect(path):
    db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=10)
    try:
        db.execute('PRAGMA query_only=ON')
        storage_kind(db)
        yield db
    finally:
        db.close()


def _tool_command(state):
    input_data = state.get('input')
    return input_data.get('command', '') if isinstance(input_data, dict) else ''


def current_session(db, call_id):
    require(re.fullmatch(r'[a-f0-9]{24,64}', call_id) is not None, 'Нужна новая уникальная метка --call-id: 24–64 шестнадцатеричных символа.')
    # Ищем только вызов инструмента с этой меткой. Не выбираем сессию по времени.
    pattern = re.compile(r'--call-id\s+[\"\']?' + re.escape(call_id) + r'(?=[\s\"\']|$)')
    matches = []
    if storage_kind(db) == 'v2':
        for sid, raw in db.execute('SELECT session_id,data FROM session_message WHERE data LIKE ?', ('%' + call_id + '%',)):
            try:
                message = json.loads(raw)
            except ValueError:
                continue
            content = message.get('content')
            if not isinstance(content, list):
                continue
            for part in content:
                if not (isinstance(part, dict) and part.get('type') == 'tool'):
                    continue
                state = part.get('state') or {}
                command = _tool_command(state) if isinstance(state, dict) else ''
                if pattern.search(command) and 'sonar_ingest.py' in command:
                    matches.append((sid, state.get('status')))
    else:
        for sid, raw in db.execute("SELECT session_id,data FROM part WHERE json_extract(data,'$.type')='tool' AND data LIKE ?", ('%' + call_id + '%',)):
            part = json.loads(raw)
            state = part.get('state', {})
            command = _tool_command(state)
            if pattern.search(command) and 'sonar_ingest.py' in command:
                matches.append((sid, state.get('status')))
    require(len(matches) == 1 and matches[0][1] == 'running',
            'Не удалось однозначно связать команду с текущей сессией OpenCode. Приём остановлен; не выбирайте другую сессию и не ищите файлы.')
    return matches[0][0]


def _tool_output_text(content):
    # OpenCode 2 хранит вывод инструмента в state.content — массиве
    # объектов {type: "text", text: "..."}; извлекаем из него текст.
    if isinstance(content, list):
        return '\n'.join(p.get('text', '') for p in content
                         if isinstance(p, dict) and p.get('type') == 'text' and isinstance(p.get('text'), str))
    return content if isinstance(content, str) else ''


def _norm_part(part):
    # OpenCode 2 хранит вывод инструмента в state.content вместо state.output.
    if part.get('type') == 'tool' and isinstance(part.get('state'), dict):
        state = part['state']
        if 'output' not in state and 'content' in state:
            part = dict(part)
            state = dict(state)
            state['output'] = _tool_output_text(state.get('content'))
            part['state'] = state
    return part


def _session_exists(db, session_id):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for table in ('session', 'session_v2'):
        if table in tables and db.execute('SELECT 1 FROM ' + table + ' WHERE id=?', (session_id,)).fetchone():
            return True
    return False


def history(db, session_id):
    require(_session_exists(db, session_id), 'Сессия OpenCode не найдена.')
    rows = []
    if storage_kind(db) == 'v2':
        order = 'time_created, seq, id' if 'seq' in table_columns(db, 'session_message') else 'time_created, id'
        for mid, raw in db.execute('SELECT id,data FROM session_message WHERE session_id=? ORDER BY ' + order, (session_id,)):
            message = json.loads(raw)
            if 'agent' in message:
                rows.append({'id': mid, 'role': 'assistant',
                             'parts': [_norm_part(p) for p in message.get('content') or [] if isinstance(p, dict)]})
            else:
                text = message.get('text')
                rows.append({'id': mid, 'role': 'user',
                             'parts': [{'type': 'text', 'text': text}] if isinstance(text, str) else []})
        return rows
    for mid, raw in db.execute('SELECT id,data FROM message WHERE session_id=? ORDER BY time_created,id', (session_id,)):
        message = json.loads(raw)
        parts = [json.loads(row[0]) for row in db.execute('SELECT data FROM part WHERE session_id=? AND message_id=? ORDER BY time_created,id', (session_id, mid))]
        rows.append({'id': mid, 'role': message.get('role'), 'parts': parts})
    return rows


def user_text(message):
    return '\n'.join(p.get('text', '') for p in message['parts'] if p.get('type') == 'text' and not p.get('synthetic') and not p.get('ignored')).strip()


def extract_json(text):
    text = text.strip().lstrip('\ufeff')
    if text.startswith('```'):
        match = re.fullmatch(r'```(?:json)?\s*\n(.*?)\n```', text, flags=re.S | re.I)
        require(match is not None, 'Незавершённый блок JSON.')
        text = match.group(1).strip()
    try:
        def invalid_constant(value):
            raise ValueError('Недопустимая JSON-константа')
        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError('Повтор поля JSON')
                result[key] = value
            return result
        data = json.loads(text, parse_constant=invalid_constant, object_pairs_hook=unique_object)
    except (ValueError, RecursionError):
        raise IntakeError('Сообщение не содержит корректный JSON SonarQube.') from None
    require(isinstance(data, dict), 'JSON SonarQube должен быть объектом.')
    return text, data


def validate_page(data, source_url=None):
    require(not data.get('errors') and not data.get('error'), 'SonarQube вернул ошибку API.')
    require(isinstance(data.get('issues'), list), 'В JSON отсутствует массив issues.')
    components = data.get('components', [])
    require(isinstance(components, list) and all(isinstance(c, dict) and isinstance(c.get('key'), str) for c in components), 'Некорректный массив components.')
    paging = data.get('paging')
    require(isinstance(paging, dict), 'В JSON отсутствует paging.')
    values = [data.get('total'), paging.get('total'), paging.get('pageIndex'), paging.get('pageSize')]
    require(all(type(v) is int for v in values), 'Поля total, paging.total, pageIndex и pageSize должны быть целыми числами.')
    total, pt, page, size = values
    require(total >= 0 and total == pt and page >= 1 and 1 <= size <= 100, 'Некорректные значения total или paging.')
    require(page <= max(1, (total + size - 1) // size), 'Номер страницы выходит за пределы выгрузки.')
    require(len(data['issues']) == min(size, max(0, total - (page - 1) * size)), 'Страница неполна: число issues не соответствует total и paging.')
    require(('p' not in data or data['p'] == page) and ('ps' not in data or data['ps'] == size), 'Поля p/ps противоречат paging.')
    keys = []
    for issue in data['issues']:
        require(isinstance(issue, dict) and all(isinstance(issue.get(k), str) and issue[k] for k in ('key', 'rule', 'component')), 'В issues нет обязательных key, rule или component.')
        line = issue.get('line', 0)
        require(line is None or (type(line) is int and line >= 0), 'Некорректная строка замечания.')
        require(issue.get('severity') is None or isinstance(issue['severity'], str), 'Некорректная серьёзность замечания.')
        keys.append(issue['key'])
    require(len(set(keys)) == len(keys), 'На странице есть повторяющиеся ключи замечаний.')
    if source_url:
        query = parse_qs(urlsplit(api_url(source_url)).query)
        project = query.get('componentKeys', query.get('projects', ['']))[0]
        pr = query.get('pullRequest', [None])[0]
        branch = query.get('branch', [None])[0]
        require(project and ',' not in project, 'Для приёма нужна ссылка на один проект SonarQube.')
        evidence = data['issues'] or [c for c in data.get('components', []) if c.get('qualifier') == 'TRK']
        require(bool(evidence), 'JSON не содержит данных для проверки проекта и PR.')
        for item in evidence:
            actual = item.get('project', item.get('key') if item.get('qualifier') == 'TRK' else None)
            require(actual == project, 'Проект JSON не соответствует исходной ссылке SonarQube.')
            require((str(item.get('pullRequest')) if item.get('pullRequest') is not None else None) == pr, 'PR JSON не соответствует исходной ссылке SonarQube.')
            if branch is not None:
                require(item.get('branch') == branch, 'В JSON невозможно подтвердить ветку исходной ссылки.')
        for component in data.get('components', []):
            require(component['key'] == project or component['key'].startswith(project + ':'), 'В components обнаружен другой проект.')
            require((str(component.get('pullRequest')) if component.get('pullRequest') is not None else None) == pr, 'В components обнаружен другой PR.')
    return page, size, total


def select_payloads(messages):
    users = [(n, m, user_text(m)) for n, m in enumerate(messages) if m['role'] == 'user']
    require(bool(users), 'Нет пользовательского сообщения.')
    last_n, last, last_text = users[-1]
    # Последнее пользовательское сообщение обязательно должно быть JSON, а не
    # инструкция с примером; назад к старому корректному JSON не откатываемся.
    extract_json(last_text)
    def source_link(text):
        match = re.fullmatch(r'https?://\S+', text)
        if not match:
            match = re.fullmatch(r'/sonar\s+(https?://\S+)', text)
            if not match:
                match = re.search(r'^Аргумент команды:[ \t]*(https?://\S+)[ \t]*$', text, re.M)
            text = match.group(1) if match else ''
        return text if urlsplit(text).path in ('/project/issues', '/api/issues/search') else None
    anchors = [(n, m, source_link(t)) for n, m, t in users[:-1] if source_link(t)]
    require(bool(anchors), 'В этой сессии перед JSON нет исходной ссылки SonarQube.')
    anchor_n, anchor, source = anchors[-1]
    expected_api = api_url(source)
    handoff = any(m['role'] == 'assistant' and p.get('type') == 'tool'
                  and 'sonar_index.py' in p.get('state', {}).get('input', {}).get('command', '')
                  and 'api-url' in p.get('state', {}).get('input', {}).get('command', '')
                  and p.get('state', {}).get('status') == 'completed'
                  and expected_api in p.get('state', {}).get('output', '')
                  and 'WAITING_FOR_USER_JSON' in p.get('state', {}).get('output', '')
                  for m in messages[anchor_n + 1:last_n] for p in m['parts'])
    require(handoff, 'В этой сессии не подтверждена выдача API-ссылки перед JSON.')
    pages = {}
    for n, message, text in users:
        if n <= anchor_n:
            continue
        # Повторно вставленная корректная страница заменяет прежнюю в памяти.
        if n != last_n and not (text.startswith('{') or text.startswith('```')):
            continue
        try:
            raw, data = extract_json(text)
            page, size, total = validate_page(data, source)
        except IntakeError:
            if n == last_n:
                raise
            continue
        pages[page] = {'raw': raw, 'data': data, 'messageID': message['id']}
    settings = {validate_page(p['data'], source)[1:] for p in pages.values()}
    require(len(settings) == 1, 'У страниц различаются total или pageSize; вставьте согласованную полную выгрузку.')
    return source, anchor['id'], pages


def immutable(path, text):
    if path.exists():
        require(path.read_text(encoding='utf-8') == text, 'Конфликт сохранённых данных; существующий файл не изменён.')
        return
    # Атомарная публикация. Параллельные импорты исключены блокировкой сессии.
    fd, temp = tempfile.mkstemp(prefix='.sonar-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as stream:
            stream.write(text)
        os.replace(temp, path)
    finally:
        if Path(temp).exists():
            Path(temp).unlink()


def ingest(db, session_id, reports):
    source, anchor_id, pages = select_payloads(history(db, session_id))
    reports = Path(reports).resolve()
    reports.mkdir(parents=True, exist_ok=True)
    lock = reports / (session_id + '.lock')
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise IntakeError('Приём этой сессии уже выполняется; параллельный запуск остановлен.') from None
    os.close(fd)
    try:
        total = next(iter(pages.values()))['data']['total']
        size = next(iter(pages.values()))['data']['paging']['pageSize']
        required = set(range(1, max(1, (total + size - 1) // size) + 1))
        issues = [i for n in sorted(pages) for i in pages[n]['data']['issues']]
        require(len({i['key'] for i in issues}) == len(issues), 'Между страницами повторяются ключи замечаний.')
        identity = hashlib.sha256((session_id + anchor_id + source).encode()).hexdigest()[:16]
        page_paths = []
        for n, payload in sorted(pages.items()):
            digest = hashlib.sha256(payload['raw'].encode()).hexdigest()[:16]
            path = reports / ('sonar-' + identity + '-' + digest + '-p' + str(n) + '.json')
            immutable(path, payload['raw'])
            page_paths.append(str(path))
        missing = sorted(required - set(pages))
        if missing:
            base = api_url(source)
            return {'status': 'WAITING_FOR_USER_JSON', 'received': len(issues), 'total': total,
                    'missingPages': missing, 'urls': [re.sub(r'([?&])p=\d+', r'\g<1>p=' + str(n), base) for n in missing]}
        require(len(issues) == total, 'Выгрузка неполна; индексация остановлена.')
        index = build_index(issues, total)
        data_id = hashlib.sha256(''.join(Path(p).name for p in page_paths).encode()).hexdigest()[:16]
        query = parse_qs(urlsplit(api_url(source)).query)
        project = query.get('componentKeys', query.get('projects', ['']))[0]
        target = 'pr-' + query['pullRequest'][0] if 'pullRequest' in query else 'branch-' + query.get('branch', ['main'])[0]
        safe = lambda s: re.sub(r'[^\w.-]', '-', s)[:120]
        stem = safe(project) + '__' + safe(target) + '__' + identity + '-' + data_id
        index_path = reports / (stem + '-index.json')
        state_path = reports / (stem + '-state.json')
        combined_path = reports / (stem + '-issues.json')
        # Объединённый JSON предназначен для существующих compare/queue/skipped.
        combined = {'total': total, 'issues': issues, 'sourceUrl': source, 'sourcePages': page_paths}
        dump = lambda obj: json.dumps(obj, ensure_ascii=False, indent=2)
        immutable(combined_path, dump(combined))
        immutable(index_path, dump(index))
        if not state_path.exists():
            immutable(state_path, dump({'state': 'in_progress', 'sourceUrl': source, 'sessionID': session_id,
                      'sourceMessageIDs': [pages[n]['messageID'] for n in sorted(pages)],
                      'issuesPath': str(combined_path), 'indexPath': str(index_path),
                      'decisions': {}, 'groups': {}}))
        return {'status': 'READY', 'sessionID': session_id,
                'messageIDs': [pages[n]['messageID'] for n in sorted(pages)],
                'received': len(issues), 'total': total, 'complete': True,
                'modules': len(index['modules']), 'severities': dict(Counter(i.get('severity', '?') for i in issues)),
                'issuesPath': str(combined_path), 'indexPath': str(index_path), 'statePath': str(state_path)}
    finally:
        lock.unlink()


def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--call-id', required=True, help='Уникальная метка текущего вызова shell')
    args = parser.parse_args()
    try:
        with connect(database_path()) as db:
            result = ingest(db, current_session(db, args.call_id), Path.home() / '.config/opencode/sonar-reports')
        print(json.dumps(result, ensure_ascii=False))
    except (IntakeError, sqlite3.Error, OSError, ValueError) as exc:
        print(json.dumps({'status': 'ERROR', 'message': str(exc),
                          'action': 'Остановитесь. Если ошибка относится к JSON, попросите вставить полный JSON повторно одним сообщением. Не ищите файлы и не повторяйте команду в этом ходе.'}, ensure_ascii=False))
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
