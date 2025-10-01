package db

type keyOffset struct {
	len    int
	offset int
}

type entryOffset struct {
	len    int
	offset int
}

type dataOffset struct {
	key   keyOffset
	entry entryOffset
}
