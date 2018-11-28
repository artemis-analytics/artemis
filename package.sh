DIR=$PWD #Default value for DIR. 

#Argument passing, currently only one argument is possible.
#Additional options are expected over time.
POSITIONAL=()
while [[ $# -gt 0 ]]
do
	key="$1"
	case $key in
		-d|--dir|--directory) #Output directory for .tar.gz file.
			DIR="${2:-$(pwd)}"
			echo "$DIR"
			shift
			shift
			;;
esac
done
set -- "${POSTIONAL[@]}"

#Help option to explain program.
if [[ $1 == 'help' || $1 == '-h' || $1 == '--help' ]]
then
	echo "Usage:"
	echo "How to use, TODO"
	exit 0
fi

#Actual work done.
python setup.py sdist bdist_wheel
pip freeze > ./dist/requirements.txt
tar -cvf artemis.tar ./dist/
mkdir -p $DIR
gzip -c artemis.tar > "$DIR"/artemis.tar.gz
rm -r artemis.egg-info build dist artemis.tar
