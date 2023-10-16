package main

//go:generate mockery --name DB
type DB interface {
	Get(val string) string
}
