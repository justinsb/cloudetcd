Based on https://kubernetes.io/docs/tasks/administer-cluster/certificates/


MASTER_IP=127.0.0.1
MASTER_CLUSTER_IP=100.64.0.1

openssl genrsa -out ca.key 2048

# TODO: Why MASTER_IP?

openssl req -x509 -new -nodes -key ca.key -subj "/CN=${MASTER_IP}" -days 10000 -out ca.crt


cat >> csr.conf <<EOF
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[ dn ]
#C = <country>
#ST = <state>
#L = <city>
#O = <organization>
#OU = <organization unit>
CN = ${MASTER_IP}

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = kubernetes
DNS.2 = kubernetes.default
DNS.3 = kubernetes.default.svc
DNS.4 = kubernetes.default.svc.cluster
DNS.5 = kubernetes.default.svc.cluster.local
IP.1 = ${MASTER_IP}
IP.2 = ${MASTER_CLUSTER_IP}

[ v3_ext ]
authorityKeyIdentifier=keyid,issuer:always
basicConstraints=CA:FALSE
keyUsage=keyEncipherment,dataEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=@alt_names
EOF

openssl req -new -key server.key -out server.csr -config csr.conf

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt -days 10000 \
    -extensions v3_ext -extfile csr.conf -sha256

openssl req  -noout -text -in ./server.csr

# Create admin key
openssl genrsa -out admin.key 2048

openssl req -new -key admin.key -out admin.csr -subj "/CN=admin-user/O=system:masters"

openssl x509 -req -in admin.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out admin.crt -days 365 -sha256



Based on https://nakamasato.medium.com/run-kubernetes-api-server-locally-64d56f6299ff

# service account
openssl genrsa -out service-account-key.pem 4096
openssl req -new -x509 -days 365 -key service-account-key.pem -subj "/CN=test" -sha256 -out service-account.pem

# api-server
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -subj "/CN=test" -days 10000 -out ca.crt
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -config csr.conf
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt -days 10000 \
    -extensions v3_ext -extfile csr.conf
    


 go run ./cmd/kube-apiserver --cert-dir ./run --etcd-servers=http://127.0.0.1:2379 \
   --service-account-issuer=https://127.0.0.1:6443 \
  --service-account-key-file=./run/service-account-key.pem \
  --service-account-signing-key-file=./run/service-account-key.pem \
  --tls-cert-file=./run/server.crt \
  --tls-private-key-file=./run/server.key \
  --client-ca-file=./run/ca.crt








${PATH_TO_KUBERNETES_DIR}/_output/bin/kube-apiserver --etcd-servers http://localhost:2379 \
--service-account-key-file=service-account-key.pem \
--service-account-signing-key-file=service-account-key.pem \
--service-account-issuer=api \
--tls-cert-file=server.crt \
--tls-private-key-file=server.key \
--client-ca-file=ca.crt



# service account
openssl genrsa -out service-account-key.pem 4096
openssl req -new -x509 -days 365 -key service-account-key.pem -subj "/CN=test" -sha256 -out service-account.pem

# api-server
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -subj "/CN=test" -days 10000 -out ca.crt
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -config csr.conf
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt -days 10000 \
    -extensions v3_ext -extfile csr.conf

# kubeconfig
kubectl config set-cluster local-apiserver \
--certificate-authority=run/ca.crt \
--embed-certs=true \
--server=https://127.0.0.1:6443

kubectl config set-credentials admin \
--client-certificate=run/admin.crt \
--client-key=run/admin.key \
--embed-certs=true

kubectl config set-context default \
--cluster=local-apiserver \
--user=admin

kubectl config use-context default
