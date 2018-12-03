DIR=$PWD # Default value for DIR.

# Argument passing.
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
		-v|--version) # Name of the project in the format 'artemis-.X.X.X'.
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

# Help option with explanation.
if [[ $1 == 'help' || $1 == '-h' || $1 == '--help' ]]
then
	echo "Usage:"
	echo "bash setup.sh -t <type of installation> -v <name of project and version>"
        echo "Type of installation:"
        echo "cli: source code and command line tools"
        echo "package: artemis installed as a package to be included in programs"
        echo "Version:"
        echo "Should be of the format artemis-X.Y.Z."
	exit 0
fi

# Function that installs Artemis with source code available.
cli(){
	tar -xzvf artemis.tar.gz
	pip install -r ./dist/requirements.txt --no-index --find-links file://$DIR/dist/
	tar -xzvf "dist/$VERSION.tar.gz"
	python $VERSION/setup.py install
}

# Function that installs Artemis as a package.
package(){
	tar -xzvf artemis.tar.gz
	pip install ./dist/artemis.whl
}

# Determines which installation type the user wants.
if [ $INSTALL = "package" ]
then
	package
else
	cli
fi
