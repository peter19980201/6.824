package kvraft

type db struct {
	m map[string]string
}

func (db *db) put(key, value string) {
	db.m[key] = value
}

func (db *db) append(key, value string) {
	db.m[key] += value
}

func (db *db) get(key string) (string, Err) {
	if _, ok := db.m[key]; !ok {
		return "", ErrNoKey
	}
	//fmt.Println("dbKey:", key, db.m[key])
	return db.m[key], OK
}
