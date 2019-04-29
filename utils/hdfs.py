import subprocess
import  re


class DirectoryNotFoundException(Exception):
    pass
class CannotDeleteFileException(Exception):
    pass
class UnmergedDirectoryException(Exception):
    pass

# check if file or directory exists
def check_exists(path) :
    """

    :param path:
    :return:
    """
    cmd = ['hadoop', 'fs', '-test', '-e', path]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode

# return a size in megabytes of a given file or directory
def get_size(path):
    """

    :param path:
    :return:
    """
    if  check_exists(path):
        raise DirectoryNotFoundException('%s is not found.' % path)
    cmd = ['hadoop', 'fs', '-du', '-s', path]
    output = subprocess.check_output(cmd).split()
    size = int(output[0])
    return int(size/1024/1024)


def get_hadoop_file(sc,input_file,delimiter):
    """

    :param sc:
    :param input_file:
    :param delimiter:
    :return:
    """
    rdd = sc.newAPIHadoopFile(
            input_file,
            'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
            'org.apache.hadoop.io.LongWritable',
            'org.apache.hadoop.io.Text',
            conf={'textinputformat.record.delimiter': delimiter},
        )
    return rdd


def delete_file(file_path):
    """

    :param file_path:
    :return:
    """
    cmd = ['hadoop', 'fs', '-rm', '-r', '-f', file_path]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode


def merge_directory(directory_path,destination_file):
    """

    :param directory_path:
    :param destination_file:
    :return:
    """
    cmd = ['hadoop' , 'fs' ,'-getmerge' , directory_path + '/*',destination_file]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    proc.communicate()
    if proc.returncode:
        raise UnmergedDirectoryException('Cannot merge %s directory in %s file.' % (directory_path , destination_file))


def merge_file(source_file,destination_file,delete_source=True):
    """

    :param source_file:
    :param destination_file:
    :param delete_source:
    :return:
    """
    input = subprocess.Popen(['hadoop', 'fs', '-cat', source_file],stdout=subprocess.PIPE)
    output = subprocess.Popen(['hadoop', 'fs', '-put', '-', destination_file],stdin=subprocess.PIPE)
    for line in input.stdout:
        output.stdout.write(line)
    input.stdout.close()
    input.wait()
    output.stdin.close()
    output.wait()
    if delete_source:
        return_code = delete_file(source_file)
        if return_code:
            raise CannotDeleteFileException('Cannot delete %s file.' % source_file)





