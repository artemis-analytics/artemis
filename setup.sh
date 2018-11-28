DIR=$PWD

POSITIONAL=()
while [[ $# -gt 0 ]]
do
	key="$1"
	case $key in
		-d|--dir|--directory)
			DIR="${2:-$(pwd)}"
			echo "$DIR"
			shift
			shift
			;;
		-v|--version)
			VERSION="${2}"
			echo "$VERSION"
			shift
			shift
			;;
		-t|--type)
			INSTALL="${2}"
			echo "$INSTALL"
			shift
			shift
			;;
esac
done
set -- "${POSITIONAL[@]}"

if [[ $1 == 'help' || $1 == '-h' || $1 == '--help' ]]
then
	echo "Usage:"
	echo "How to use, TODO"
	exit 0
fi

cli(){
	tar -xzvf artemis.tar.gz
	pip install -r ./dist/requirements.txt
	tar -xzvf "dist/$VERSION.tar.gz"
	python $VERSION/setup.py install
}

package(){
	tar -xzvf artemis.tar.gz
	pip install ./dist/artemis.whl
}

if [ $INSTALL = "package" ]
then
	package
else
	cli
fi
