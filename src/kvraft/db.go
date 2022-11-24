package kvraft

type db struct {
	M map[string]string
}

func (db *db) put(key, value string) {
	db.M[key] = value
}

func (db *db) append(key, value string) {
	db.M[key] += value
}

func (db *db) get(key string) (string, Err) {
	if _, ok := db.M[key]; !ok {
		return "", ErrNoKey
	}
	//fmt.Println("dbKey:", key, db.m[key])
	return db.M[key], OK
}
