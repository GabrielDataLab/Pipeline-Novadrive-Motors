
#!/bin/bash
# ============================================
# NovaDrive Motors — EC2 Setup
# Instala Docker e sobe Apache Airflow 3
# Testado em Ubuntu 22.04 (AWS EC2)
# ============================================
 
# 1. Atualizar lista de pacotes
sudo apt-get update
 
# 2. Instalar dependências para repositório via HTTPS
sudo apt-get install -y ca-certificates curl gnupg lsb-release
 
# 3. Criar diretório para chaves de repositório
sudo mkdir -m 0755 -p /etc/apt/keyrings
 
# 4. Adicionar chave GPG do Docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
 
# 5. Adicionar repositório do Docker
echo "deb [arch=$(dpkg --print-architecture) \
  signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
 
# 6. Atualizar lista de pacotes após adicionar repositório do Docker
sudo apt-get update
 
# 7. Instalar Docker e componentes
sudo apt-get install -y \
  docker-ce \
  docker-ce-cli \
  containerd.io \
  docker-buildx-plugin \
  docker-compose-plugin
 
# 8. Baixar docker-compose.yaml oficial do Airflow
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
 
# 9. Criar diretórios necessários
mkdir -p ./dags ./logs ./plugins
 
# 10. Criar .env com UID do usuário (necessário para permissões do Docker)
echo -e "AIRFLOW_UID=$(id -u)" > .env
 
# 11. Desativar exemplos de DAGs (editar manualmente se necessário)
# No arquivo docker-compose.yaml, alterar:
# AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
 
# 12. Subir Airflow em modo desacoplado
sudo docker compose up -d
 
# ============================================
# Aguarde todos os containers ficarem healthy.
# Acesse o Airflow em:
# http://<EC2_PUBLIC_DNS>:8080
# Usuário padrão: airflow / airflow