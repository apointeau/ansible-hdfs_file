#! /usr/bin/env python

# @Author: Antoine Pointeau
# @Date:   2017-04-24T14:10:08+02:00
# @Email:  web.pointeau@gmail.com
# @Filename: hdfs.py
# @Last modified by:   Antoine Pointeau
# @Last modified time: 2017-06-19T14:17:43+02:00

ANSIBLE_METADATA = {
    "metadata_version"  : "1.0",
    "supported_by"      : "community",
    "status"            : ["preview"],
    "version"           : "1.0.0"
}

DOCUMENTATION = """
---
module: hdfs_file
short_description: Set attributes of files in the hadoop file system
description:
    - This module allows file/directory management.
    - It assert the status for a given path, like owner, group, mode.
version_added: "2.2"
author: Antoine Pointeau (@apointeau)
options:
    group:
        description:
            - Name of the group that should own the
            - file/directory, as would be fed to chown.
    method:
        description:
            - If `command`, this module will manage with the hdfs CLI.
            - If `library`, this module will use the python package `hdfs`.
        default: command
        choices:
            - command
            - library
    mode:
        description:
            - Mode the file or directory should be. For those used to
            - /usr/bin/chmod remember that modes are actually octal numbers
            - (eg. 0644). Noe the this fonction will always set changed
            - due to CLI limitation (cannot get file mode)
    owner:
        description:
            - Name of the user that should own the
            - file/directory, as would be fed to chown.
    path:
        description:
            - The hdfs absolute path being managed
        required: true
        aliases:
            - dest
            - name
    recurse:
        description:
            - The module will recursively set the specified file
            - attributes (applies only to state=directory)
    replication:
        description:
            - The replication factor of a file/directory, please
            - not that setting the replication on a directory is
            - always apply recursivly.
    state:
        description:
            - If `directory`, all immediate subdirectories will
            - be created if they do not exist, they will be created
            - with the supplied permissions.
            - If `file`, the file will NOT be created if it does not exist,
            - see the `hdfs_copy` module if you want that behavior.
            - If `absent`, directories will be recursively deleted,
            - and files will be unlinked. Note that `hdfs_file` will not fail
            - if the path does not exist as the state did not change.
            - If `touch`, an empty file will be created if the path does
            - not exist, while an existing file will receive updated file
            - access and modification times (directories stay untouch). Please
            - note that touch update file owner, group, mode and replication,
            - you should set this param explicitly to replace them.
        default: file
        choices:
            - directory
            - file
            - absent
            - touch
notes:
    - check_mode supported
    - method `library` not yet supported
"""

EXAMPLES = """

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

"""

import sys

from ansible.module_utils.basic import AnsibleModule

from ansible.module_utils.cyres.HdfsUtils import (
    HdfsUtilsError,
    HdfsCheckMode
)

def build_module():
    argument_spec = dict(
        group       = dict(default=None, required=False),
        method      = dict(
            default="command", required=False,
            choices=["command", "library"]
        ),
        mode        = dict(default=None, required=False),
        owner       = dict(default=None, required=False),
        path        = dict(aliases=["dest", "name"], required=True),
        recurse     = dict(default=None, required=False),
        replication = dict(default=None, required=False),
        state       = dict(
            default="file", required=False,
            choices=["file", "directory", "touch", "absent"]
        ),
    )
    mutually_exclusive = []
    module = AnsibleModule(
        argument_spec=argument_spec,
        mutually_exclusive=mutually_exclusive,
        supports_check_mode=True
    )
    return (module)


def resolv_states(module, params, context, status):
    old = status["state"]
    new = params["state"]
    try:
        if old == "absent" and new == "file":
            module.fail_json(msg="no such file, to create new file use state 'touch' instead of 'file'")
        elif old == "absent" and new == "directory":
            context.mkdir(params["path"], parent=True)
        elif new == "touch" and old in ["absent", "file"]:
            context.touch(params["path"])
        elif new == "absent" and old == "file":
            context.remove(params["path"])
        elif new == "absent" and old == "directory":
            context.remove(params["path"], recurse=True)
        else:
            module.fail_json(msg="unsupported state convert '%s' -> '%s'" % (old, new))
    except HdfsUtilsError as e:
        module.fail_json(msg=e)
    return True


def should_modify(status, params, value):
    if params[value] is None:
        return False
    if params["state"] == "touch":
        return True
    if str(status[value]) == str(params[value]):
        return False
    return True


def main():
    module = build_module()
    params = module.params
    changed = False

    if params["method"] == "command":
        context = HdfsContextCli()
    elif params["method"] == "library":
        # context = HdfsContextLib()
        module.fail_json(msg="Not yet implemented")
    if module.check_mode:  # Neutralize context fonctions
        context = HdfsCheckMode(context)

    status = context.stats(params["path"])
    # STATE
    if status["state"] != params["state"]:
        changed = True
        resolv_states(module, params, context, status)
    if params["state"] == "absent":
        module.exit_json(changed=changed)
    # OWNER
    if should_modify(status, params, "owner"):
        changed = True
        context.chown(params["path"], owner=params["owner"], recurse=params["recurse"])
    # GROUP
    if should_modify(status, params, "group"):
        changed = True
        context.chown(params["path"], group=params["group"], recurse=params["recurse"])
    # REPLICATION
    if should_modify(status, params, "replication"):
        changed = True
        context.setrep(params["path"], params["replication"])
    # MODE
    if params["mode"] is not None:
        changed = True
        context.chmod(params["path"], params["mode"], recurse=params["recurse"])

    module.exit_json(changed=changed)


if __name__ == "__main__":
    main()
