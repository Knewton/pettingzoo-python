#!/bin/bash

set -e

function usage {
	echo >&2 "usage: $0 [-k] [-a application]"
	echo >&2 "    -k  Keep the virtualenv; don't destroy it, if it exists."
	echo >&2 "    -a  If the application name is FooBar, then the virtualenv"
	echo >&2 "        directory will be ${venvdir}/FooBar. This defaults to the"
	echo >&2 "        name of the folder this script lives in (e.g. $(basename $appdir))."
	exit 1
}

if [[ -d /var/local/pip_download_cache ]]; then
	export PIP_DOWNLOAD_CACHE=/var/local/pip_download_cache
else
	export PIP_DOWNLOAD_CACHE=$HOME/.pip_download_cache
fi

venvdir="/opt/virtualenvs"
appdir="$(cd "$(dirname $0)/..";pwd)"
cfgdir="$appdir/config"
application=$(basename "$appdir")
python_bin="python2"
destroy=true
while getopts a:p:hk opt; do
	case "$opt" in
		a)  application="$OPTARG";;
		p)  python_bin="$OPTARG";;
		h)  usage;;
		k)  destroy=false;;
		\?) usage;;
	esac
done
shift `expr $OPTIND - 1`

cd "$venvdir"

if $destroy; then
	rm -rf $application
fi

if [ ! -e $application ]; then
	virtualenv --never-download --system-site-packages --distribute --python=$python_bin $application
fi

source $application/bin/activate
reqs=( \
	"requirements.external.txt" \
	"requirements.internal.txt" \
	"requirements.testing.txt" \
)
requirements=$(mktemp)
for req in ${reqs[@]}; do
	if [ -e "$cfgdir/$req" ]; then
		cat "$cfgdir/$req" >> $requirements || true
	fi
done
# remove comments
sed -i '/^\s*#/d' $requirements
# remove blank lines
sed -i '/^\s*$/d' $requirements
if [ -s $requirements ]; then
	pip install --requirement $requirements --index-url https://pypi.knewton.net/simple
fi
rm -f $requirements

# libraries under development
if [ -e "$cfgdir/requirements.dev.txt" ]; then
	tmpfile=$(mktemp)
	grep -v '^\s*$' "$cfgdir/requirements.dev.txt" | grep -v '^\s*#' > $tmpfile || true
	while read path; do
		pushd $path > /dev/null
		rm -rf dist
		./setup.py sdist
		pip install dist/*
		popd > /dev/null
	done < $tmpfile
fi

deactivate
