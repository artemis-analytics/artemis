DIR=$PWD # Default value for DIR.


# Help option with explanation.
call_help(){
	echo "Usage:"
	echo "bash setup.sh -t <type of installation> -v <name of project and version>"
        echo "Type of installation:"
        echo "unpack: Run setup.sh directly on artemis tarball."
        echo "setup: Artemis tarball already extracted, run setup.sh in presence of dist folder."
        echo "Version:"
        echo "Should be of the format artemis-X.Y.Z."
	exit 0
}

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
		-h|--help|help)
			call_help
			shift
			shift
			;;
esac
done
set -- "${POSITIONAL[@]}"


# Function that unpacks Artemis tarball.
unpack(){
	tar -xzvf artemis.tar.gz
}

# Function that installs Artemis after unpacking.
setup(){
	pip install -r ./dist/requirements.txt --no-index --find-links file://$DIR/dist/
	tar -xzvf "dist/$VERSION.tar.gz"
	python $VERSION/setup.py install
}

# Determines which installation type the user wants.
if [ $INSTALL = "unpack" ]
then
	unpack
	setup
else
	setup
fi
