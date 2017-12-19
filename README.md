# ansible-hdfs_file

Ansible module that manipulates files and directories in hdfs using the CLI.

This module come from my company internal Ansible toolbox. [Groupe Cyrès][1]

We write a small article in french to explain the CLI choice. [link][2]

[1]: https://www.cyres.fr/

[2]: https://www.cyres.fr/blog/hadoop-ansible-episode-1-hdfs/

# Contributor

- Antoine Pointeau : Creator
- Hadrien Puissant : Testers

# Instruction

1) Copy the file `./libary/hdfs_file.py` into the Ansible library folder. (located at: `/etc/ansible/library` dy default)

2) Copy the directory `./module_utils` into the `/etc/ansible` folder.  
*This directory is planed to be use in further Ansible versions*

3) Create a synlink named `cyres` in the Ansible package module_utils folder pointing on the `/etc/ansible/module_utils`.  
If you installed Ansible in the linux default python2.7, try:  
```
ln -s /etc/ansible/module_utils /usr/lib/python2.7/site-packages/ansible/module_utils/cyres
```  
*You can change the symlink name but you have to modify the import line 117 of ./library/hdfs_file.py*

*If you're not able to create this synlink copy the content of HdfsUtils.py at the beginning of hdfs_file.py and delete the import at the line 117*

# Usage

Examples:
```
- name: Create folder
  hdfs_file:
    path: /tmp/myfolder
    state: directory
    owner: myuser
    group: mygroup
    mode: 0755

- name: Delete the folder
  hdfs_file:
    path: /tmp/myfolder
    state: absent

- name: Create file
  hdfs_file:
    path: /tmp/myfile
    state: touch
```
**More details in the file ./library/hdfs_files.py**
