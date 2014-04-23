#!
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook  -i ansible_hosts_sns -u arye --module-path=modules/ -f 100 $@; date
