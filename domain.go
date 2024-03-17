package main

type DataType int32

type MediaProgress struct {
	UserId       string
	DataType     string
	TitleId      string
	MediaId      string
	Milliseconds int64
	EventAt      int64
}
