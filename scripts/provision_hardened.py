#!/usr/bin/env python3
"""Generate a NEW isolated deployment's private CA and distinct credentials.

Never rotates an existing directory or modifies development state. Supply externally
managed certificates instead for a real deployment; keep the CA key offline.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
from urllib.parse import quote


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--test-only', action='store_true')
    args = parser.parse_args()
    root = args.directory.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    def write(name, data, mode=0o444):
        path = root/name
        path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        if path.exists():
            if path.read_text() == data: return
            raise RuntimeError("Refusing to overwrite generated credential material")
        path.write_text(data); path.chmod(mode)
    def run(*command, input=None):
        result = subprocess.run(command, input=input, text=True, capture_output=True)
        if result.returncode: raise RuntimeError('Credential provisioning command failed (output withheld)')
        return result.stdout
    write('authority/placeholder','',0o600)
    (root/'authority').chmod(0o700)
    ca = root/'authority/ca'
    run('openssl','req','-x509','-newkey','rsa:3072','-nodes','-days','365','-subj','/CN=SOC isolated deployment CA',
        '-keyout',str(ca)+'.key','-out',str(ca)+'.crt')
    (root/'authority/ca.key').chmod(0o600)
    for service in ('postgres','elasticsearch','web','kafka','kafka-admin','producer','spark','ml','storage','alerts','threat','consumer'):
        directory = root/service; directory.mkdir(exist_ok=True,mode=0o755)
        run('openssl','req','-new','-newkey','rsa:2048','-nodes','-subj',f'/CN={service}',
            '-keyout',str(directory/'server.key'),'-out',str(directory/'server.csr'))
        hosts=f'DNS:{service}' + (',DNS:localhost,IP:127.0.0.1' if service=='web' else '')
        write(service+'/extensions.cnf',f'subjectAltName={hosts}\nextendedKeyUsage=serverAuth,clientAuth\n')
        run('openssl','x509','-req','-in',str(directory/'server.csr'),'-CA',str(ca)+'.crt','-CAkey',str(ca)+'.key',
            '-CAcreateserial','-days','90','-out',str(directory/'server.crt'),'-extfile',str(directory/'extensions.cnf'))
        (directory/'server.key').chmod(0o444)
        write(service+'/ca.crt',(root/'authority/ca.crt').read_text())
    if args.test_only: write('test-only', 'Isolated security verification only.\n')
    passwords={name:secrets.token_urlsafe(32) for name in ('postgres','elastic','backend','storage','alerts','threat','prometheus','grafana')}
    for name in ('postgres','elastic','grafana'):
        write(('elasticsearch' if name=='elastic' else name)+'/password',passwords[name]+'\n')
    for name in ('backend','storage','alerts','threat'):
        value={'database_url':f'postgresql+psycopg://{name}:{quote(passwords[name])}@postgres:5432/soc?sslmode=verify-full&sslrootcert=/run/security/ca.crt',
               'elasticsearch':{'username':name,'password':passwords[name],'ca_certs':'/run/security/ca.crt'},
               'prometheus':{'username':name,'password':passwords[name]}}
        write(name+'/security.json',json.dumps(value))
        write(name+'/ca.crt',(root/'authority/ca.crt').read_text())
    write('backend/metrics-token',secrets.token_urlsafe(32)+'\n')
    write('prometheus/metrics-token',(root/'backend/metrics-token').read_text())
    # Password is passed through stdin, never a process argument.
    auth='basic_auth_users:\n'
    for name in ('backend','grafana'):
        hashed=run('htpasswd','-niB',name,input=passwords[name]+'\n').strip().split(':',1)[1]
        auth+='  '+name+': '+hashed+'\n'
    write('prometheus/web.yml',auth)
    write('grafana/datasource.yml', 'apiVersion: 1\ndatasources:\n  - name: Prometheus\n    type: prometheus\n    access: proxy\n    url: http://prometheus:9090\n    basicAuth: true\n    basicAuthUser: grafana\n    secureJsonData:\n      basicAuthPassword: '+passwords['grafana']+'\n')
    write('admin/bootstrap.json',json.dumps({'passwords':passwords}))
    write('admin/ca.crt',(root/'authority/ca.crt').read_text())
    write('postgres/pg_hba.conf','local all all trust\nhostssl soc all 0.0.0.0/0 scram-sha-256\nhostssl soc all ::/0 scram-sha-256\nhostnossl all all 0.0.0.0/0 reject\nhostnossl all all ::/0 reject\n')
    print('Generated isolated credentials. No existing data, credentials or containers were modified.')


if __name__ == '__main__': main()
