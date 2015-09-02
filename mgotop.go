package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type Stat struct {
	Time  int
	Count int
}

type CollStat struct {
	Total, Queries, Getmore, Insert, Update, Remove, Commands Stat
	ReadLock                                                  Stat `bson:"readLock"`
	WriteLock                                                 Stat `bson:"writeLock"`
}

type Colls map[string]CollStat

type MgoTop struct {
	Totals Colls
	Ok     int
}

type Diff struct {
	Name                                                                           string
	Total, ReadLock, WriteLock, Queries, Getmore, Insert, Update, Remove, Commands int
	Sort                                                                           int
}

type ByDiff []Diff

func (b ByDiff) Len() int {
	return len(b)
}

func (b ByDiff) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByDiff) Less(i, j int) bool {
	return b[i].Sort < b[j].Sort
}

func Calc(last, current *MgoTop, sortBy string, isSortByTime bool) (diffs ByDiff) {
	for collName, currentStat := range current.Totals {
		lastStat, ok := last.Totals[collName]
		if !ok {
			continue
		}
		var diff Diff
		if isSortByTime {
			diff = Diff{
				Name:      collName,
				Total:     currentStat.Total.Time - lastStat.Total.Time,
				Insert:    currentStat.Insert.Time - lastStat.Insert.Time,
				Update:    currentStat.Update.Time - lastStat.Update.Time,
				Remove:    currentStat.Remove.Time - lastStat.Remove.Time,
				Queries:   currentStat.Queries.Time - lastStat.Queries.Time,
				Getmore:   currentStat.Getmore.Time - lastStat.Getmore.Time,
				Commands:  currentStat.Commands.Time - lastStat.Commands.Time,
				ReadLock:  currentStat.ReadLock.Time - lastStat.ReadLock.Time,
				WriteLock: currentStat.WriteLock.Time - lastStat.WriteLock.Time,
			}

		} else {
			diff = Diff{
				Name:      collName,
				Total:     currentStat.Total.Count - lastStat.Total.Count,
				Insert:    currentStat.Insert.Count - lastStat.Insert.Count,
				Update:    currentStat.Update.Count - lastStat.Update.Count,
				Remove:    currentStat.Remove.Count - lastStat.Remove.Count,
				Queries:   currentStat.Queries.Count - lastStat.Queries.Count,
				Getmore:   currentStat.Getmore.Count - lastStat.Getmore.Count,
				Commands:  currentStat.Commands.Count - lastStat.Commands.Count,
				ReadLock:  currentStat.ReadLock.Count - lastStat.ReadLock.Count,
				WriteLock: currentStat.WriteLock.Count - lastStat.WriteLock.Count,
			}
		}
		switch sortBy {
		case "total", "totals":
			diff.Sort = diff.Total
		case "insert":
			diff.Sort = diff.Insert
		case "update":
			diff.Sort = diff.Update
		case "remove":
			diff.Sort = diff.Remove
		case "query", "queries":
			diff.Sort = diff.Queries
		case "getmore":
			diff.Sort = diff.Getmore
		case "command", "commands":
			diff.Sort = diff.Commands
		case "rlock", "readlock":
			diff.Sort = diff.ReadLock
		case "wlock", "writelock":
			diff.Sort = diff.WriteLock
		}
		diffs = append(diffs, diff)
	}
	sort.Sort(sort.Reverse(diffs))
	return
}

func Show(diffs ByDiff, sortKey string, limit int, first, isSortByTime bool) {
	if !first {
		fmt.Printf("\033[%dA\r", limit+2)
	}
	cond := "count"
	if isSortByTime {
		cond = "time"
	}
	fmt.Printf("=================== sort: %s %s ===================\n", sortKey, cond)
	fmt.Println("total\trlock\twlock\tquery\tinsert\tupdate\tremove\tgetmore\tcommand\tns")
	for i := 0; i < limit && i < len(diffs); i++ {
		fmt.Printf("\033[2K%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n", diffs[i].Total,
			diffs[i].ReadLock,
			diffs[i].WriteLock,
			diffs[i].Queries,
			diffs[i].Insert,
			diffs[i].Update,
			diffs[i].Remove,
			diffs[i].Getmore,
			diffs[i].Commands,
			diffs[i].Name)
	}
}

func init() {
	go func() {
		// make sure user input will not effect display
		reader := bufio.NewReader(os.Stdin)
		for {
			_, err := reader.ReadString('\n')
			if err != nil {
				log.Fatal(err)
			}
			fmt.Print("\033[1A\033[2K\r")
		}
	}()
}

func main() {
	var (
		host         = flag.String("h", "127.0.0.1:27017", "host")
		sortKey      = flag.String("k", "total", "sort key")
		isSortByTime = flag.Bool("t", false, "sort by time?")
		limit        = flag.Int("n", 20, "show top n")
		sleepTime    = flag.Float64("s", 1, "sleep between each show")
	)
	flag.Parse()
	conn, err := mgo.Dial(*host)
	if err != nil {
		log.Fatal(err)
	}
	var lastTop *MgoTop
	var first bool = true
	for {
		m := &MgoTop{}
		err = conn.DB("admin").Run(bson.M{"top": 1}, m)
		if err != nil {
			log.Fatal(err)
		}
		if m.Ok == 0 {
			log.Fatal(m)
		}
		if lastTop == nil {
			lastTop = m
			time.Sleep(time.Duration(*sleepTime*1000) * time.Millisecond)
			continue
		}
		diffs := Calc(lastTop, m, strings.ToLower(*sortKey), *isSortByTime)
		Show(diffs, *sortKey, *limit, first, *isSortByTime)
		first = false
		lastTop = m
		time.Sleep(time.Duration(*sleepTime*1000) * time.Millisecond)
	}
}
