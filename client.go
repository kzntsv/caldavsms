package caldavsms

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	dac "github.com/Snawoot/go-http-digest-auth-client"
	iso8601 "github.com/dylanmei/iso8601"
	"github.com/emersion/go-webdav/caldav"
	db "github.com/sonyarouje/simdb"
	"github.com/teambition/rrule-go"
)

const (
	dateFormat        = "20060102"
	datetimeFormat    = "20060102T150405"
	datetimeUTCFormat = "20060102T150405Z"
)

var (
	dlocation   string
	dmintime    time.Time
	dfirsttoken string
)

type calendarItemPath struct {
	Path     string
	IsActual bool
}
type calendarItemPaths struct {
	CalendarItemPaths *[]calendarItemPath
	Client            *client
	CalendarPath      *string
}
type trigger struct {
	Uid     string `json:"uid"`
	Trigger string `json:"trigger"`
}
type phone struct {
	Phone string `json:"phone"`
}
type exdate struct {
	Exdate   string    `json:"exdate"`
	DateTime time.Time `json:"datetime"`
}
type event struct {
	Tzid        string     `json:"tzid"`
	Uid         string     `json:"uid"`
	Description string     `json:"description"`
	Reccurence  string     `json:"reccurence"`
	Dtstart     string     `json:"dtstart"`
	Exdates     *[]exdate  `json:"exdates"`
	Rrule       string     `json:"rrule"`
	Status      string     `json:"status"`
	Kind        string     `json:"kind"`
	Triggers    *[]trigger `json:"triggers"`
	PhonesSMS   *[]phone   `json:"phonessms"`
	TextSMS     string     `json:"textsms"`
}
type events struct {
	Events *[]event
}
type task struct {
	DateTime   time.Time `json:"datetime"`
	Uid        string    `json:"uid"`
	UidTrigger string    `json:"uidtrigger"`
}
type tasks struct {
	Task *[]task
}
type message struct {
	Phone string `json:"phone"`
	Text  string `json:"text"`
}
type props struct {
	Id       string    `json:"id"`
	DateTime time.Time `json:"datetime"`
	Token    string    `json:"token"`
}
type driver struct {
	Driver *db.Driver
}
type digitalAuthHTTPClient struct {
	c httpClient
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type client struct {
	Client *caldav.Client
}

func (c *digitalAuthHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return c.c.Do(req)
}

func httpClientWithDigitalAuth(c httpClient) httpClient {
	if c == nil {
		c = http.DefaultClient
	}
	return &digitalAuthHTTPClient{c}
}

// Функция возвращает текущее время
func getCurrentTime() (time.Time, error) {
	currenttime := toTime(time.Now(), dlocation)
	if dmintime.IsZero() {
		panic("The mintime-parameter is not specified")
	} else if currenttime.Before(dmintime) {
		panic("The current time is less than the mintime-parameter")
	} else {
		return currenttime, nil
	}
}

// Функция инициализирует файловое хранилище
func initDriver(storagename string) (*driver, error) {
	var err error
	d, err := db.New(storagename)
	if err != nil {
		return nil, err
	}
	return &driver{Driver: d}, nil
}

func (d *driver) writePropsDB(t time.Time, token string) error {
	db := &props{DateTime: t, Token: token, Id: "0"}
	if err := db.writeDB(d.Driver); err != nil {
		return err
	}
	return nil
}

// Функция возвращает из хранилища предыдущую дату синхронизации и токен
// В случае, если в хранилище нет информации, создает новые данные
// время - текущее время, токен - firsttoken
func (d *driver) getPropsDB() (*props, error) {
	var db *props
	currenttime, err := getCurrentTime()
	if err != nil {
		return nil, err
	}

	if dfirsttoken == "" {
		return nil, fmt.Errorf("Не задан первоначальный токен синхронизации")
	}
	if err := d.Driver.Open(props{}).First().AsEntity(&db); err != nil {
		db = &props{DateTime: currenttime, Token: dfirsttoken, Id: "0"}
		if err := db.writeDB(d.Driver); err != nil {
			return nil, err
		} else {
			if currenttime.Before(db.DateTime) {
				return nil, fmt.Errorf("Время, указанное в хранилище больше, чем текущее время")
			}
		}
	} else if db.DateTime.Before(dmintime) {
		return nil, fmt.Errorf("Время, указанное в хранилище меньше, чем минимальное допустимое")
	}
	return db, nil
}

// Функция выполняет запись параметров синхронизации в хранилище
func (sp *props) writeDB(driver *db.Driver) error {
	if err := driver.Upsert(sp); err != nil {
		return err
	} else {
		return nil
	}
}

func (sp props) ID() (jsonField string, value interface{}) {
	{
		value = sp.Id
		jsonField = "id"
		return
	}
}

// Функция принимает на вход клиента, путь к календарю и возвращает новый токен календаря
func (cl *client) getNewCalendarToken(calendarpath string) (string, error) {
	if token, err := cl.Client.GetToken(context.Background(), calendarpath); err != nil {
		return "", err
	} else {
		return token, nil
	}
}

// Функция принимает на вход интерфейс, который может принимать тип string форматов "20060102",  "20060102T150405", "20060102T150405Z"
// или тип time.Time, и вторым - локализацию в виде строки вида "Europe/Moscow". Возвращает время в приведенном формате.
func toTime(value interface{}, loc string) time.Time {
	if loc == "" {
		loc = dlocation
	}
	l, err := time.LoadLocation(loc)
	if err != nil {
		panic(err)
	}
	switch v := value.(type) {
	case string:
		switch len(v) {
		case 8:
			if t, err := time.ParseInLocation(dateFormat, v, l); err != nil {
				panic(err)
			} else {
				return t.UTC().In(l)
			}
		case 15:
			if t, err := time.ParseInLocation(datetimeFormat, v, l); err != nil {
				panic(err)
			} else {
				return t
			}
		case 16:
			if t, err := time.ParseInLocation(datetimeUTCFormat, v, time.UTC); err != nil {
				panic(err)
			} else {
				return t.UTC().In(l)
			}
		default:
			panic("Некорректный формат строкового значения value")
		}
	case time.Time:
		return v.UTC().In(l)
	default:
		panic("Некорректный тип параметра value")
	}
}

// Функция получает на вход имя, пароль, путь к сервису календарей и выполняет инициализацию клиента с digist-авторизацией
func newClient(username, password, uri string) (*client, error) {
	httpClient := &http.Client{
		Transport: dac.NewDigestTransport(username, password, http.DefaultTransport),
	}
	authorizedClient := httpClientWithDigitalAuth(httpClient)
	if caldavClient, err := caldav.NewClient(authorizedClient, uri); err == nil {
		return &client{Client: caldavClient}, nil
	} else {
		return nil, err
	}
}

// Функция получает на вход клиента, имя календаря и возвращает путь к календарю
func (cl *client) getCalendarPath(calendarname string) (string, error) {
	principal, err := cl.Client.FindCurrentUserPrincipal(context.Background())
	if err != nil {
		return "", err
	}
	homeSet, err := cl.Client.FindCalendarHomeSet(context.Background(), principal)
	if err != nil {
		return "", err
	}
	calendars, err := cl.Client.FindCalendars(context.Background(), homeSet)
	if err != nil {
		return "", err
	}
	var cal caldav.Calendar
	for _, c := range calendars {
		if c.Name == calendarname {
			cal = c
		}
	}
	if cal.Path == "" {
		msg := "Не найден календарь с именем '" + calendarname + "'"
		return "", fmt.Errorf("%v", msg)
	}
	return cal.Path, nil
}

// Функция получает на вход клиента, путь к календарю, токен и возвращает ссылку на слайс путей к событиям календаря
func (cl *client) getCalendarChanges(calendarpath, token string) (*calendarItemPaths, error) {
	ms, err := cl.Client.GetCalendarChanges(context.Background(), calendarpath, token)
	if err != nil {
		return nil, err
	}
	var cc []calendarItemPath
	for _, m := range ms.Responses {
		var c calendarItemPath
		if m.Status != nil {
			if m.Status.Text == "Not Found" {
				c = calendarItemPath{Path: m.Hrefs[0].Path, IsActual: false}
			}
		} else if m.PropStats != nil {
			if m.PropStats[0].Status.Text == "OK" {
				c = calendarItemPath{Path: m.Hrefs[0].Path, IsActual: true}
			}
		} else {
			continue
		}
		cc = append(cc, c)
	}
	return &calendarItemPaths{CalendarItemPaths: &cc, Client: cl, CalendarPath: &calendarpath}, nil
}

// Функция выполняет удаление только "плохих" путей
func (cs *calendarItemPaths) deleteNotActualPathsDB(driver *driver) error {
	for _, c := range *cs.CalendarItemPaths {
		if !c.IsActual && c.Path != "" {
			uid := c.Path[len(*cs.CalendarPath) : len(c.Path)-len(".ics")]
			e := event{Uid: uid}
			e.DeleteDB(driver)
			m := task{Uid: uid}
			m.DeleteDB(driver)
		}
	}
	return nil
}

// Функция по слайсу ссылок на календарь идет на сервер caldav, получает объекты и возвращает их в виде *Events
// В этой функции стоит фильтр, чтобы не брались события, которые удалены
func (cs *calendarItemPaths) getEvents() (*events, error) {
	var paths []string
	for _, c := range *cs.CalendarItemPaths {
		if c.IsActual {
			paths = append(paths, c.Path)
		}
	}
	if len(paths) != 0 {
		mc, err := cs.Client.Client.MultiGetCalendar(context.Background(), *cs.CalendarPath, &caldav.CalendarMultiGet{Paths: paths})
		if err != nil {
			return nil, err
		}
		var evs []event
		for _, ical := range mc {
			var tzid string
			for _, e := range ical.Data.Component.Children {
				if e.Name == "VTIMEZONE" {
					tzid = e.Props.Get("TZID").Value
				}
				if e.Name == "VEVENT" || e.Name == "VTODO" {
					uid := e.Props.Get("UID").Value
					var description string
					if e.Props.Get("DESCRIPTION") != nil {
						description = e.Props.Get("DESCRIPTION").Value
					}
					var recurrence string
					if e.Props.Get("RECURRENCE-ID") != nil {
						recurrence = e.Props.Get("RECURRENCE-ID").Value
					}
					var dtstart string
					if e.Props.Get("DTSTART") != nil {
						dtstart = e.Props.Get("DTSTART").Value
					}
					var exdates []exdate
					if e.Props.Get("EXDATE") != nil {
						for _, ex := range e.Props.Values("EXDATE") {
							s := exdate{Exdate: ex.Value}
							exdates = append(exdates, s)
						}
					}
					var rrule string
					if e.Props.Get("RRULE") != nil {
						rrule = e.Props.Get("RRULE").Value
					}
					var status string
					if e.Props.Get("STATUS") != nil {
						status = e.Props.Get("STATUS").Value
					}
					event := event{Tzid: tzid, Uid: uid, Description: description, Reccurence: recurrence, Dtstart: dtstart, Exdates: &exdates, Rrule: rrule, Kind: e.Name, Status: status}
					var tr []trigger
					for _, a := range e.Children {
						t := trigger{Uid: a.Props.Get("UID").Value, Trigger: a.Props.Get("TRIGGER").Value}
						tr = append(tr, t)
					}
					event.Triggers = &tr
					event.calc()
					evs = append(evs, event)
				}
			}
		}
		return &events{Events: &evs}, nil
	}
	return &events{Events: &[]event{}}, nil
}

// Функция выполняет расчет дополнительных полей event
func (ev *event) calc() {
	if ev.Description != "" {
		t, phs := ev.parseDescription()
		ev.TextSMS = t
		if phs != nil {
			ev.PhonesSMS = phs
		}
	} else {
		ev.PhonesSMS = &[]phone{}
	}
	for i, _ := range *ev.Exdates {
		(*ev.Exdates)[i].DateTime = toTime((*ev.Exdates)[i].Exdate, "")
	}
}

// Функция извлекасет из Description объекта Event текст сообщения и список номеров
func (ev *event) parseDescription() (string, *[]phone) {
	s := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(ev.Description, "\\;", ";"), "\\,", ","), "\\n", " "), "\\\\", "\\"), "  ", " "), "\t", " ")
	ss := strings.SplitN(s, ":", 3)
	if len(ss) == 3 {
		pref := strings.ToUpper(ss[0])
		var phs []phone
		if pref == "SMS" || pref == "СМС" {
			re := regexp.MustCompile("[;,]")
			spt := re.Split(ss[1], -1)
			for _, p := range spt {
				pp := parsePhone(p)
				if pp != "" {
					pn := phone{Phone: pp}
					phs = append(phs, pn)
				}
			}
		}
		re := regexp.MustCompile("[А-Яа-я]+?")
		isRussian := re.MatchString(ss[2])
		r := []rune(ss[2])
		cnt := len(r)
		var t string
		if isRussian {
			if cnt > 70 {
				t = string(r[:69]) + ">"
			} else {
				t = string(r)
			}
			return t, &phs
		} else {
			if cnt > 160 {
				t = string(r[:159]) + ">"
			} else {
				t = string(r)
			}
			return t, &phs
		}
	}
	return "", &[]phone{}
}

func parsePhone(p string) string {
	s := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(p, "(", ""), ")", ""), "-", ""), " ", "")
	re := regexp.MustCompile(`^[+]?[0-9]+$`)
	if re.MatchString(s) {
		if len(s) == 12 && s[0:2] == "+7" {
			s = "8" + s[2:]
			return s
		} else if s[0:1] == "+" && len(s) > 12 {
			s = "810" + s[1:]
			return s
		} else if len(s) >= 4 {
			return s
		} else {
			return ""
		}
	} else {
		return ""
	}
}

// Функция выполняет расчет событий, следующих после заданного в параметре времени и возвращает ссылку на Messages
func (ev *events) calcMessages(dateTimeStartSync time.Time) *tasks {
	var ts []task
	if ev.Events != nil {
		for _, e := range *ev.Events {
			if e.isForSMS() {
				dtstartdatetime := toTime(e.Dtstart, e.Tzid)
				if e.Rrule != "" {
					r, _ := rrule.StrToRRule(e.Rrule)
					r.DTStart(dtstartdatetime)
					triggerTimeNew := toTime("99991231T000000", e.Tzid)
					var uidTrigger string
					var flag bool
				outer:
					for _, tr := range *e.Triggers {
						d := r.GetDTStart()
						triggerTime := tr.parseTriggerTime(d, e.Tzid)
						if triggerTime.After(dateTimeStartSync) {
							if triggerTime.Before(triggerTimeNew) && ev.IsRruleDate(&e, d) {
								flag = true
								triggerTimeNew = triggerTime
								uidTrigger = tr.Uid
							}
							if !tr.isNegative() && d.Before(dateTimeStartSync) {
								d = dateTimeStartSync
								for {
									d = r.Before(d, false)
									if d.IsZero() {
										continue outer
									}
									if !ev.IsRruleDate(&e, d) {
										continue
									}
									triggerTime = tr.parseTriggerTime(d, e.Tzid)
									if triggerTime.After(dateTimeStartSync) {
										if triggerTime.Before(triggerTimeNew) {
											flag = true
											triggerTimeNew = triggerTime
											uidTrigger = tr.Uid
										}
										continue
									}
									continue outer
								}
							}
						} else if tr.isNotAbs() {
							for {
								d = r.After(d, false)
								if d.IsZero() {
									continue outer
								}
								if !ev.IsRruleDate(&e, d) {
									continue
								}
								triggerTime = tr.parseTriggerTime(d, e.Tzid)
								if triggerTime.After(dateTimeStartSync) {
									if triggerTime.Before(triggerTimeNew) {
										flag = true
										triggerTimeNew = triggerTime
										uidTrigger = tr.Uid
									}
									continue outer
								}
							}
						}
					}
					if flag {
						t := task{DateTime: triggerTimeNew, Uid: e.Uid, UidTrigger: uidTrigger}
					outer1:
						for {
							for i := range ts {
								if t.Uid != ts[i].Uid {
									continue
								} else {
									if t.DateTime.Before(ts[i].DateTime) {
										ts[i] = t
										break outer1
									} else {
										break outer1
									}
								}
							}
							ts = append(ts, t)
							break outer1
						}
					}
				} else {
					if e.Dtstart != "" {
						triggerTimeNew := toTime("99991231T000000", e.Tzid)
						var flag bool
						var uidTrigger string
						for _, tr := range *e.Triggers {
							triggerTime := tr.parseTriggerTime(dtstartdatetime, e.Tzid)
							if triggerTime.After(dateTimeStartSync) {
								if triggerTime.Before(triggerTimeNew) {
									flag = true
									triggerTimeNew = triggerTime
									uidTrigger = tr.Uid
								}
							}
						}
						if flag {
							t := task{DateTime: triggerTimeNew, Uid: e.Uid, UidTrigger: uidTrigger}
						outer2:
							for {
								for i := range ts {
									if t.Uid != ts[i].Uid {
										continue
									} else {
										if t.DateTime.Before(ts[i].DateTime) {
											ts[i] = t
											break outer2
										} else {
											break outer2
										}
									}
								}
								ts = append(ts, t)
								break outer2
							}
						}
					}
				}
			}
		}
	}
	return &tasks{Task: &ts}
}

// Функция выполняет запись Mesages в хранилище
func (ts *tasks) writeDB(driver *driver) error {
	for _, m := range *ts.Task {
		m.DeleteDB(driver)
		if err := driver.Driver.Insert(m); err != nil {
			return err
		}
	}
	return nil
}

// Функция получает из БД event по его индентификатору
func (driver *driver) getEventsByUidDB(uid string) *events {
	var result []event
	driver.Driver.Open(event{}).Where("uid", "=", uid).Get().AsEntity(&result)
	return &events{Events: &result}
}

func (ev event) DeleteDB(driver *driver) {
	driver.Driver.Delete(ev)
}

// Функция выполняет удаление событий из хранилища
func (ev *events) DeleteDB(driver *driver) {
	for _, e := range *ev.Events {
		e.DeleteDB(driver)
	}
}

// Функция выполняет запись events в хранилище
func (ev *events) writeDB(driver *driver) error {
	ev.DeleteDB(driver)
	for _, e := range *ev.Events {
		if e.isForSMS() {
			if err := driver.Driver.Insert(e); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c event) ID() (jsonField string, value interface{}) {
	{
		value = c.Uid
		jsonField = "uid"
		return
	}
}

func (c task) ID() (jsonField string, value interface{}) {
	{
		value = c.Uid
		jsonField = "uid"
		return
	}
}

// Функция выполняет удаление сообщения из Message
func (m *task) DeleteDB(driver *driver) {
	driver.Driver.Delete(m)
}

func (tr *trigger) isNotAbs() bool {
	return tr.Trigger[0:1] == "P" || tr.Trigger[1:2] == "P"
}

func (tr *trigger) isNegative() bool {
	return tr.Trigger[0:1] == "-"
}

func (tr *trigger) parseTriggerTime(dtstartdatetime time.Time, loc string) time.Time {
	/*Функция возвращает время напоминания по входному значению dtstart и дельты trigger формата:
	  trigger := "PT0S" // время события
	  trigger := "-PT2H" // за 2 часа до события
	  trigger := "-PT5M" // за 5 минут до события
	  trigger := "PT3H" // после 3 часов после события
	  trigger := "-P1D" // за 1 день до события
	  trigger := "P7D" // за 1 неделю до события
	  trigger := "-P6DT9H" // за 6 дней и 9 часов
	  trigger := "20230312T143500Z"
	*/

	trigger := tr.Trigger
	if tr.isNotAbs() {
		var sign int8 = 1
		if tr.isNegative() {
			sign = -1
			trigger = trigger[1:]
		}
		delta, err := iso8601.ParseDuration(trigger)
		if err != nil {
			panic(err)
		}
		return dtstartdatetime.Add(time.Duration(sign) * delta)
	} else {
		return toTime(trigger, loc)
	}
}

func (ev event) isForSMS() bool {
	return ev.Reccurence != "" || (ev.TextSMS != "" && len(*ev.PhonesSMS) != 0 && ev.Dtstart != "" && ev.Status != "COMPLETED")
}

// Функция проверяет, были ли переносы или удаление конкретных дат повторяющихся событий
func (ev *events) IsRruleDate(x *event, dtstartdatetime time.Time) bool {

	if x.Rrule == "" {
		panic("Нельзя проверять дату Rrule у неповторяющихся событий")
	}
	for _, e := range *ev.Events {
		if e.Uid == x.Uid && e.Rrule == "" && e.Reccurence != "" {
			if dtstartdatetime.Equal(toTime(e.Reccurence, e.Tzid)) {
				return false
			}
		}
	}
	if x.Exdates != nil {
		for _, d := range *x.Exdates {
			if (toTime(d.Exdate, x.Tzid)).Equal(dtstartdatetime) {
				return false
			}
		}
	}
	return true
}

// Функция выполняет получение сообщений из базы данных, которые идут раньше времени t
func (driver *driver) getMessagesBefore(t time.Time) (*tasks, error) {
	var result, ts []task
	if err := driver.Driver.Open(task{}).AsEntity(&result); err != nil {
		return nil, err
	}
	for _, m := range result {
		if m.DateTime.Before(t) {
			ts = append(ts, m)
		}
	}
	sort.Slice(ts, func(i, j int) (less bool) {
		return ts[i].DateTime.Before(ts[j].DateTime)
	})
	return &tasks{Task: &ts}, nil
}

// Функция выполняет рассылку сообщений
func (ts *tasks) sendMessages(driver *driver) {
	var ms []message
outer:
	for _, t := range *ts.Task {
		ev := driver.getEventsByUidDB(t.Uid)
		for _, e := range *ev.Events {
			for _, tr := range *e.Triggers {
				if t.UidTrigger == tr.Uid {
					for _, p := range *e.PhonesSMS {
						ms = append(ms, message{Phone: p.Phone, Text: e.TextSMS})
					}
					continue outer
				}
			}
		}
	}
	for _, m := range ms {
		//fmt.Println("Отправка сообщения=", m)
		http.Get("http://10.39.1.12/default/en_US/send.html?u=admin&p=admin&l=2&n=" + m.Phone + "&m=" + url.QueryEscape(m.Text))
		if len(ms) > 1 {
			time.Sleep(10 * time.Second)
		}
	}
}

func genNewMessages(driver *driver, ts *tasks, tm time.Time) {
	for _, m := range *ts.Task {
		m.DeleteDB(driver)
		ev := driver.getEventsByUidDB(m.Uid)
		mNew := ev.calcMessages(tm)
		if *mNew.Task == nil {
			ev.DeleteDB(driver)
		}
		mNew.writeDB(driver)
	}
}

// Функция получает на вход имя, пароль, адрес, имя календаря, локализацию, первый токен (для архивной загрузки),
// минимальное время (для того, чтобы из-за сбоя времени и отсутствия файла базы данных не сыпались старые СМС)
// и запускает процесс синхронизации
func Sync(username, password, uri, calendarname, location, storagename, firsttoken string, mintime time.Time) {
	dmintime = mintime
	dfirsttoken = firsttoken
	dlocation = location

	currenttime, err := getCurrentTime()
	if err != nil {
		panic(err)
	}
	driver, err := initDriver(storagename)
	if err != nil {
		panic(err)
	}
	db, err := driver.getPropsDB()
	if err != nil {
		panic(err)
	}
	client, err := newClient(username, password, uri)
	if err != nil {
		panic(err)
	}
	calendarpath, err := client.getCalendarPath(calendarname)
	if err != nil {
		panic(err)
	}
	itempaths, err := client.getCalendarChanges(calendarpath, db.Token)
	if err != nil {
		panic(err)
	}
	token, err := client.getNewCalendarToken(calendarpath)
	if err != nil {
		panic(err)
	}
	if err := itempaths.deleteNotActualPathsDB(driver); err != nil {
		panic(err)
	}
	ev, err := itempaths.getEvents()
	if err != nil {
		panic(err)
	}
	ms := ev.calcMessages(db.DateTime)
	ms.writeDB(driver)

	var evActualChanges []event
outer:
	for _, e := range *ev.Events {
		for _, m := range *ms.Task {
			if e.Uid == m.Uid {
				evActualChanges = append(evActualChanges, e)
				continue outer
			}
		}
		e.DeleteDB(driver)
		m := task{Uid: e.Uid}
		m.DeleteDB(driver)
	}
	ev = nil

	EventsActualChanges := events{Events: &evActualChanges}
	// записываем в БД только актуальные Event
	if err := EventsActualChanges.writeDB(driver); err != nil {
		panic(err)
	}

	msForSend, err := driver.getMessagesBefore(currenttime)
	if err != nil {
		panic(err)
	}
	// отправляем сообщение
	msForSend.sendMessages(driver)

	//генерируем новые даты сообщений для будущих отправок
	genNewMessages(driver, msForSend, currenttime)
	driver.writePropsDB(currenttime, token)
}
