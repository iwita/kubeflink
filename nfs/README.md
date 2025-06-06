## NFS Server Setup

Setup NFS server in a worker

```bash
sudo apt update
sudo apt install -y nfs-kernel-server
sudo mkdir -p /srv/nfs/flink
sudo chown nobody:nogroup /srv/nfs/flink
sudo chmod 777 /srv/nfs/flink
echo "/srv/nfs/flink *(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports
sudo exportfs -ra
sudo ufw allow from <cluster-subnet>/24 to any port nfs
```

On Kubernetes master
```bash
curl -skSL https://raw.githubusercontent.com/kubernetes-csi/csi-driver-nfs/v4.5.0/deploy/install-driver.sh | bash -s v4.5.0 --
```