package archive

import "os"

// shard merepresentasikan satu bagian dari cache yang di-shard.
//
// Setiap shard memuat rentang ID berurutan pada file terpisah (atau suffixed path
// bila menggunakan multi-shard).  Apabila opsi `UseMmap` aktif, field `mmap`
// berisi hasil dari `unix.Mmap` sehingga akses baca/tulis cukup melalui copy
// memori tanpa syscall I/O.
//
// Field `offset` menyimpan ID offset (1-based) dari shard pertama agar fungsi
// pencarian dapat cepat menghitung shard yang tepat.
//
// Catatan: definisi tetap tidak diekspor untuk menjaga enkapsulasi; API publik
// berinteraksi melalui RingBufferCache.
type shard struct {
	file     *os.File // descriptor file fisik
	mmap     []byte   // region memory-map (nil bila mmap dimatikan)
	filePath string   // path file pada disk
	size     int64    // jumlah record dalam shard
	offset   int64    // ID offset (basis 1) untuk shard ini
}
