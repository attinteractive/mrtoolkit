
tar:
	tar cfv ../mrtoolkit.tar -C.. --exclude=\.svn --exclude=sample-data mrtoolkit

data:
	tar cfv ../sample-data.tar -C.. --exclude=\.svn mrtoolkit/sample-data
