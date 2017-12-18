# @Author: Antoine Pointeau
# @Date:   2017-05-09T14:17:56+02:00
# @Email:  web.pointeau@gmail.com
# @Filename: HdfsUtils.py
# @Last modified by:   Antoine Pointeau
# @Last modified time: 2017-05-09T17:56:46+02:00

# --------------------------------------------------------------------------- #
# ----------------------------------------------------------------- HdfsUtils #
# --------------------------------------------------------------------------- #

"""
Catchable class to handle the Exception of this bundle
"""


class HdfsUtilsError(Exception):
    pass

# --------------------------------------------------------------------------- #
# ------------------------------------------------------------ HdfsContextCli #
# --------------------------------------------------------------------------- #


"""
This module give an hdfs context using CLI to manage the file system.
All Exception will be raised with the class `HdfsUtilsError` but
the common usage is to catch `HdfsUtilsError`.

Public methods short descriptions:
- chmod: change file/directory right mode
- chown: change file/directory owner/group
- mkdir: create a directory
- remove: delete a file/directory
- setrep: set a file/directory replication factor
- stats: get informations about a file/directory
- touch: create file or update file access
"""

import re
from subprocess import Popen, PIPE


class HdfsContextCli:

    def __init__(self, command="/bin/hdfs"):
        """
        The param :command: allow to change the CLI path, it's not common
        to set this parameter.
        """
        self.cmd = command

    def chmod(self, path, mode, recurse=None):
        """
        Change the :mode: of the given :path:. The param :mode: needs to
        be a string representation of an octal number (eg. "0744")
        """
        cmd = [self.cmd, "dfs", "-chmod", mode, path]
        if recurse:
            cmd.insert(3, "-R")
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        if proc.wait() is not 0:
            raise HdfsUtilsError("subprocess chmod stderr: %s" % err)
        return (self)

    def chown(self, path, owner=None, group=None, recurse=None):
        """
        Change the :owner: and/or :group: for the given :path:. The param
        :recurse: apply the changes for all subfiles and subfolders inside
        the give :path:. (should be a directory)
        """
        target = owner if owner else "" + ":" + group if group else ""
        cmd = [self.cmd, "dfs", "-chown", target, path]
        if recurse:
            cmd.insert(3, "-R")
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        if proc.wait() is not 0:
            raise HdfsUtilsError("subprocess chown stderr: %s" % err)
        return (self)

    def mkdir(self, path, parent=False):
        """
        Create the directory pointed by :path:, the param :parent: defines
        if immediate subdirectories should be created too. This method
        raises an HdfsUtilsError if the command failed.
        """
        cmd = [self.cmd, "dfs", "-mkdir", path]
        if parent:
            cmd.insert(3, "-p")
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        if proc.wait() is not 0:
            raise HdfsUtilsError("subprocess mkdir stderr: %s" % err)
        return (self)

    def remove(self, path, recurse=False):
        """
        Remove the file/directory pointed by :path:, the params :recurse:
        allows to force delete directories and its content. This method
        raises an HdfsUtilsError if the command failed.
        """
        cmd = [self.cmd, "dfs", "-rm", path]
        if recurse:
            cmd.insert(3, "-r")
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        if proc.wait() is not 0:
            raise HdfsUtilsError("subprocess rm stderr: %s" % err)
        return (self)

    def setrep(self, path, factor):
        """
        Set the expected replication factor of the given :path:. The param
        :factor: is expected to be a number. Please not that the CLI always
        apply the factor recursivly on directories.
        """
        cmd = [self.cmd, "dfs", "-setrep", factor, path]
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        if proc.wait() is not 0:
            raise HdfsUtilsError("subprocess setrep stderr: %s" % err)
        return (self)

    def stats(self, path):
        """
        Return a formatted dict that contains the status for the
        given :path:.
        """
        result = {
            "state": "absent",
            "owner": None,
            "group": None,
            "replication": None
        }
        cmd = [self.cmd, "dfs", "-stat", "%F[SEP]%u[SEP]%g[SEP]%r", path]
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        if proc.wait() is not 0:  # Assume no such file or directory
            return (result)
        out = out.strip().split("[SEP]")
        out_order = ["state", "owner", "group", "replication"]
        for idx, key in enumerate(out_order):  # Fill the dict
            result[key] = out[idx]
        if result["state"] == "regular file":  # Normalize file string
            result["state"] = "file"
        result["replication"] = int(result["replication"])
        return (result)

    def touch(self, path):
        """
        Create a file at the given :path:, if the file already exist it
        updates file access and modification times. This method raises
        an HdfsUtilsError if the command failed.
        """
        cmd = [self.cmd, "dfs", "-touchz", path]
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        if proc.wait() is not 0:
            raise HdfsUtilsError("subprocess touchz stderr: %s" % err)
        return (self)

# --------------------------------------------------------------------------- #
# ------------------------------------------------------------ HdfsContextLib #
# --------------------------------------------------------------------------- #


"""
This module give an hdfs context using library to manage the file system.
All Exception will be raised with the class `HdfsContextLibError` but
the common usage is to catch `HdfsUtilsError`.

Public methods short descriptions:
- mkdir: create a directory
- remove: delete a file/directory
- stats: get informations about a file/directory
- touch: create file or update file access
"""


class HdfsContextLib:
    raise NotImplementedError()

# --------------------------------------------------------------------------- #
# ------------------------------------------------------------- HdfsCheckMode #
# --------------------------------------------------------------------------- #


"""
Neutralize a given HdfsContextCli or HdfsContextLib by replacing the "action"
 methods to handle the Ansible check mode.
"""


class HdfsCheckMode:

    def __init__(self, inst):
        if not isinstance(inst, HdfsContextCli) and not isinstance(inst, HdfsContextLib):
            raise HdfsUtilsError("HdfsCheckMode expect a valid HdfsContext instance")

        def chmod(path, mode, recurse=None):
            return (self)
        setattr(inst, "chmod", chmod)

        def chown(path, owner=None, group=None, recurse=None):
            return (self)
        setattr(inst, "chown", chown)

        def mkdir(path, parent=False):
            return (self)
        setattr(inst, "mkdir", mkdir)

        def remove(path, recurse=False):
            return (self)
        setattr(inst, "remove", remove)

        def setrep(path, factor):
            return (self)
        setattr(inst, "setrep", setrep)

        def touch(path):
            return (self)
        setattr(inst, "touch", touch)

        return (inst)
