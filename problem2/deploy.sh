#!/bin/bash
# problem2/deploy.sh
# Usage: ./deploy.sh <key_file> <ec2_public_ip>
set -e
if [ $# -ne 2 ]; then
  echo "Usage: $0 <key_file> <ec2_public_ip>"
  exit 1
fi
KEY_FILE="$1"
EC2_IP="$2"
echo "Deploying to EC2 instance: $EC2_IP"
scp -i "$KEY_FILE" api_server.py ec2-user@"$EC2_IP":~
scp -i "$KEY_FILE" requirements.txt ec2-user@"$EC2_IP":~
ssh -i "$KEY_FILE" ec2-user@"$EC2_IP" << 'EOF'
  python3 -m pip install -r requirements.txt --user
  pkill -f api_server.py || true
  nohup python3 api_server.py 8080 > server.log 2>&1 &
  echo "Server started. Try: curl http://localhost:8080/papers/recent?category=cs.LG"
EOF
echo "Done. Test with: curl http://$EC2_IP:8080/papers/recent?category=cs.LG"
