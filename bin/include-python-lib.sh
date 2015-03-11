#
# Sets up the environment to include all of the common python libraries
#

: ${SCRIPT_ROOT:=$(readlink -f $(dirname $(readlink -f "$0"))/..)}
export SCRIPT_ROOT

path="$SCRIPT_ROOT/lib/python"
for egg in `ls $path/*.egg`
do
    path="$path:$egg"
done

prestoadmin_path="$SCRIPT_ROOT/src/main/python"
export PYTHONPATH="$path:$prestoadmin_path:$PYTHONPATH"
