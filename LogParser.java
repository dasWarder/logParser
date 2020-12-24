package com.javarush.task.task39.task3913;

import com.javarush.task.task39.task3913.query.*;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LogParser implements IPQuery, UserQuery, DateQuery, EventQuery, QLQuery {

    private List<User> users;
    private List<String> allStrings;
    private final Function<User, String> BY_NAME = u -> u.getName();
    private final Function<User, Date> BY_DATE = u -> u.getDate();
    private final Function<User, User> BY_USER = u -> u;
    private final Function<User, Event> BY_EVENT = u -> u.getEvent();
    private final Function<User, Status> BY_STATUS = u -> u.getStatus();
    private final Function<User, String> BY_IP = u -> u.getIp();
    private final SimpleDateFormat FORMATTER = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

    @Override
    public Set<Object> execute(String query) {
        boolean isComplex = false;
        String[] splitQuery = null;
        if(query.contains("=")) {
            splitQuery = query.split("=");
            isComplex = true;
        } else if(!query.contains("=")) {
            splitQuery = query.split(" ");
            isComplex = false;
        }
        Set<Object> resultSet = null;

        if (!isComplex) {
            resultSet = (Set<Object>) getByParam(splitQuery[1], null, null, null, null);
        } else if (isComplex) {
            if(splitQuery[1].contains("and date")) {
                 resultSet = parseStringWithValueAndDate(splitQuery[0], splitQuery[1]);
            } else {
                String value = splitQuery[1].replaceAll("\"", "").trim();
                String[] leftPart = splitQuery[0].split(" ");
                resultSet = (Set<Object>) getByParam(leftPart[1], leftPart[3], value, null, null);
            }
        }

        return resultSet;
    }

    private Set<Object> parseStringWithValueAndDate(String left, String right) {
        String[] parsedValueAndDate = right.split(" and date between ");
        String value = parsedValueAndDate[0].replaceAll("\"", "").trim();
        String[] dates = parsedValueAndDate[1].split(" and ");
        String after = dates[0].replace("\"", "").trim();
        String before = dates[1].replace("\"", "").trim();

        String[] leftPart = left.split(" ");
        return (Set<Object>) getByParam(leftPart[1], leftPart[3], value, after, before);
    }

    private <T> Set<T> getByParamWithCase(String filterParam, String value, Function<User, T> function, Date after, Date before) {
        if (filterParam.equals("user")) {
            Predicate<User> filteredByUser = u -> u.getName().equals(value);
            return filteredByPredicate(filteredByUser, u -> true, function, after == null? null : after, before == null? null : before);
        } else if (filterParam.equals("date")) {
            Predicate<User> filteredByDate = null;
            try {
                Date parsedDate = FORMATTER.parse(value);
                filteredByDate = u -> u.getDate().toString().equals(parsedDate.toString());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return filteredByPredicate(filteredByDate, u -> true, function, after == null? null : after, before == null? null : before);
        } else if (filterParam.equals("event")) {
            Predicate<User> filteredByEvent = u -> u.getEvent().toString().equals(value);
            return filteredByPredicate(filteredByEvent, u -> true, function, after == null? null : after, before == null? null : before);
        } else if(filterParam.equals("status")) {
            Predicate<User> filteredByStatus = u -> u.getStatus().toString().equals(value);
            return filteredByPredicate(filteredByStatus, u -> true, function, after == null? null : after, before == null? null : before);
        } else if (filterParam.equals("ip")) {
            Predicate<User> filteredByIp = u -> u.getIp().equals(value);
            return filteredByPredicate(filteredByIp, u -> true, function, after == null? null : after, before == null? null : before);
        }
        return null;
    }


    private <T> Set<? extends Object> getByParam(String param, String filterParam, String value, String after, String before){
        Date afterDate = null;
        Date beforeDate = null;
        try {
            afterDate = after == null? null : FORMATTER.parse(after);
            beforeDate = before == null? null : FORMATTER.parse(before);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        switch (param) {
            case "ip":
                if(value == null && filterParam == null) {
                    return getUniqueIPs(null, null);
                }
                return getByParamWithCase(filterParam, value, BY_IP, afterDate, beforeDate);
            case "user":
                if(value == null && filterParam == null) {
                    return getAllUsers();
                }
                return getByParamWithCase(filterParam, value, BY_NAME, afterDate, beforeDate);
            case "date":
                if(value == null && filterParam == null) {
                    return filteredByPredicate(u -> true, u -> true, BY_DATE, null, null);
                }
                return getByParamWithCase(filterParam, value, BY_DATE, afterDate, beforeDate);
            case "event":
                if(value == null && filterParam == null) {
                    return getAllEvents(null, null);
                }
                return getByParamWithCase(filterParam, value, BY_EVENT, afterDate, beforeDate);
            case "status":
                if(value == null && filterParam == null) {
                    return filteredByPredicate(u -> true, u -> true, BY_STATUS, null, null);
                }
                return getByParamWithCase(filterParam, value, BY_STATUS, afterDate, beforeDate);
        }
        return null;
    }


    @Override
    public Set<String> getAllUsers() {
        return users.stream().map(BY_NAME).collect(Collectors.toSet());
    }

    @Override
    public int getNumberOfUsers(Date after, Date before) {
        return filteredByPredicate(user -> true, user -> true, BY_NAME, after, before).size();
    }

    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {
        Predicate<User> filteredByEvent = u -> u.getName().equals(user);
        Function<User, Event> toEvent = u -> u.getEvent();

        return filteredByPredicate(filteredByEvent, u -> true,toEvent, after, before).size();

    }

    @Override
    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        Predicate<User> filteredByIp = u -> u.getIp().equals(ip);

        return filteredByPredicate(filteredByIp, user -> true, BY_NAME, after, before);
    }

    @Override
    public Set<String> getLoggedUsers(Date after, Date before) {
        Predicate<User> filteredByLogged = u -> u.getEvent() == Event.LOGIN;

        return filteredByPredicate(filteredByLogged, user -> true, BY_NAME, after, before);
    }

    @Override
    public Set<String> getDownloadedPluginUsers(Date after, Date before) {
        Predicate<User> filteredByPlugin = u -> u.getEvent() == Event.DOWNLOAD_PLUGIN;

        return filteredByPredicate(filteredByPlugin, user -> true, BY_NAME, after, before);
    }

    @Override
    public Set<String> getWroteMessageUsers(Date after, Date before) {
        Predicate<User> filteredByMessage = u -> u.getEvent() == Event.WRITE_MESSAGE;

        return filteredByPredicate(filteredByMessage, user -> true, BY_NAME, after, before);
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before) {
       Predicate<User> filteredBySolved = u -> u.getEvent() == Event.SOLVE_TASK;

       return filteredByPredicate(filteredBySolved, user -> true, BY_NAME, after, before);
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before, int task) {
        Predicate<User> filteredBySolved = u -> u.getEvent() == Event.SOLVE_TASK;
        Predicate<User> filteredByTask = u -> u.getCode() == task;

        return filteredByPredicate(filteredBySolved, filteredByTask, BY_NAME, after, before);
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before) {
        Predicate<User> filteredByDone = u -> u.getEvent() == Event.DONE_TASK;

        return filteredByPredicate(filteredByDone, user -> true, BY_NAME, after, before);
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before, int task) {
        Predicate<User> filteredByDone = u -> u.getEvent() == Event.DONE_TASK;
        Predicate<User> filteredByTask = u -> u.getCode() == task;

        return filteredByPredicate(filteredByDone, filteredByTask, BY_NAME, after, before);
    }

    @Override
    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        Predicate<User> filteredByUser = u -> u.getName().equals(user);
        Predicate<User> filteredByEvent = u -> u.getEvent() == event;

        return filteredByPredicate(filteredByUser, filteredByEvent, BY_DATE, after, before);
    }

    @Override
    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        Predicate<User> filteredByStatus = u -> u.getStatus() == Status.FAILED;

        return filteredByPredicate(filteredByStatus, u -> true, BY_DATE, after, before);
    }

    @Override
    public Set<Date> getDatesWhenErrorHappened(Date after, Date before) {
        Predicate<User> filteredByStatus = u -> u.getStatus() == Status.ERROR;

        return filteredByPredicate(filteredByStatus, u -> true, BY_DATE, after, before);
    }

    @Override
    public Date getDateWhenUserLoggedFirstTime(String user, Date after, Date before) {
        Set<Date> dates = returnFilteredSet(BY_DATE, after, before, user, Event.LOGIN);

        return sortDates(dates);
    }

    @Override
    public Date getDateWhenUserSolvedTask(String user, int task, Date after, Date before) {
        Predicate<User> filteredByTask = u -> u.getCode() == task;

        Set<User> users = returnFilteredSet(BY_USER, after, before, user, Event.SOLVE_TASK);
        Set<Date> usersByDate = filteredByPredicateWithAddParam(users, filteredByTask, BY_DATE);
        return sortDates(usersByDate);
    }

    @Override
    public Date getDateWhenUserDoneTask(String user, int task, Date after, Date before) {
        Predicate<User> filteredByTask = u -> u.getCode() == task;

        Set<User> users = returnFilteredSet(BY_USER, after, before, user, Event.DONE_TASK);
        Set<Date> usersByDate = filteredByPredicateWithAddParam(users, filteredByTask, BY_DATE);
        return sortDates(usersByDate);
    }

    @Override
    public Set<Date> getDatesWhenUserWroteMessage(String user, Date after, Date before) {
        return returnFilteredSet(BY_DATE, after, before, user, Event.WRITE_MESSAGE);
    }

    @Override
    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        return returnFilteredSet(BY_DATE, after, before, user, Event.DOWNLOAD_PLUGIN);
    }

    @Override
    public int getNumberOfAllEvents(Date after, Date before) {

        return getAllEvents(after, before).size();
    }

    @Override
    public Set<Event> getAllEvents(Date after, Date before) {
        Predicate<User> filteredByEvent = u -> u.getEvent() != null;

        return filteredByPredicate(filteredByEvent, u -> true, BY_EVENT, after, before);
    }

    @Override
    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        Predicate<User> filteredByIp = u -> u.getIp().equals(ip);

        return filteredByPredicate(filteredByIp, u -> true, BY_EVENT, after, before);
    }

    @Override
    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        Predicate<User> filteredByUser = u -> u.getName().equals(user);

        return filteredByPredicate(filteredByUser, u -> true, BY_EVENT, after, before);
    }

    @Override
    public Set<Event> getFailedEvents(Date after, Date before) {
        Predicate<User> filteredByStatus = u -> u.getStatus() == Status.FAILED;

        return filteredByPredicate(filteredByStatus, u -> true, BY_EVENT, after, before);
    }

    @Override
    public Set<Event> getErrorEvents(Date after, Date before) {
        Predicate<User> filteredByEvent = u -> u.getStatus() == Status.ERROR;

        return filteredByPredicate(filteredByEvent, u -> true, BY_EVENT, after, before);
    }

    @Override
    public int getNumberOfAttemptToSolveTask(int task, Date after, Date before) {
        return returnFilteredSet(BY_USER, after, before, Event.SOLVE_TASK, task).size();
    }

    @Override
    public int getNumberOfSuccessfulAttemptToSolveTask(int task, Date after, Date before) {
        Predicate<User> filteredByEvent = u -> u.getEvent() == Event.DONE_TASK;
        Predicate<User> filteredByTask = u -> u.getCode() == task;
        return filteredByPredicate(filteredByEvent, filteredByTask, BY_USER, after, before).size();
    }

    @Override
    public Map<Integer, Integer> getAllSolvedTasksAndTheirNumber(Date after, Date before) {
        return filteredAndMapped(Event.SOLVE_TASK, after, before);
    }

    @Override
    public Map<Integer, Integer> getAllDoneTasksAndTheirNumber(Date after, Date before) {
        return filteredAndMapped(Event.DONE_TASK, after, before);
    }

    private Map<Integer, Integer> filteredAndMapped(Event event, Date after, Date before) {
        List<User> filteredByDate = getTimeFrame(after, before, users);

        return filteredByDate.stream()
                .filter(u -> u.getEvent() == event)
                .collect(Collectors.groupingBy(User::getCode, Collectors.summingInt(x -> 1)));
    }

    private <T> Set<T> filteredByPredicate(Predicate<User> predicate, Predicate<User> optionalPredicate, Function<User, T> function, Date after, Date before) {
        List<User> filteredByDate = getTimeFrame(after, before, users);
        return  filteredByDate.stream()
                .filter(predicate)
                .filter(optionalPredicate)
                .map(function)
                .collect(Collectors.toSet());
    }

    private <T> Set<T> filteredByPredicateWithAddParam(Set<User> set, Predicate<User> filter, Function<User, T> function) {
        return set.stream()
                .filter(filter)
                .map(function)
                .collect(Collectors.toSet());
    }

    private <T, U> Set<T> returnFilteredSet(Function<User, T> function, Date after, Date before, U ... params) {
        List<Predicate<User>> filters = new LinkedList<>();
        for(U param : params) {
            if(param instanceof Event) {
                filters.add(u -> u.getEvent() == param);
            } else if (param instanceof String){
                filters.add(u -> u.getName().equals(param));
            } else if (param instanceof Status) {
                filters.add(u -> u.getStatus() == param);
            } else if (param instanceof Integer) {
                filters.add(u -> u.getCode() == param);
            }
        }
        return filteredByPredicate(filters.get(0), filters.get(1), function, after, before);
    }

    private Date sortDates(Set<Date> dates) {
        return dates.stream()
                .sorted()
                .findFirst().orElse(null);
    }


    public LogParser(Path logDir) {
        allStrings = new ArrayList<>();
        readFromDirectory(logDir);
        users = parseFile(allStrings);
    }


    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {
        return getUniqueIPs(after, before).size();
    }
    

    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {
        List<User> timeFrame = getTimeFrame(after, before, users);
        Set<String> uniqIps = new HashSet<>();

        for(User user : timeFrame) {
            uniqIps.add(user.getIp());
        }
        return uniqIps;
    }

    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {
         return getFilteredByParam(after, before, "name", user);
    }

    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        return getFilteredByParam(after, before, "event", event.name());
    }


    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        return getFilteredByParam(after, before, "status", status.name());
    }

    private List<String> readFromDirectory(Path logDir) {
        try (DirectoryStream<Path> directories = Files.newDirectoryStream(logDir)) {
            for(Path file : directories) {
                if(file.toString().endsWith(".log") && Files.isRegularFile(file)) {
                    allStrings.addAll(Files.readAllLines(file));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    private List<User> parseFile(List<String> allLinesFromFile) {
        List<User> users = new ArrayList<>();
        for(String line : allLinesFromFile) {
            SimpleDateFormat fileDateFormatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
            User user = new User();

            String[] parsedLine = line.split("\t");
            user.setIp(parsedLine[0]);
            user.setName(parsedLine[1]);
            try {
                user.setDate(fileDateFormatter.parse(parsedLine[2]));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            String event = parseByRegex(parsedLine[3], "\\d").trim();
            String code = parseByRegex(parsedLine[3], "\\D");
            String status = parsedLine[4];

            user.setEvent(!event.equals("")? Event.valueOf(event) : null);
            user.setCode(!code.equals("")? Integer.parseInt(code) : null);
            user.setStatus(!status.equals("")? Status.valueOf(status) : null);

            users.add(user);
        }
        return users;
    }


    private String parseByRegex(String line, String regex) {
        return line.replaceAll(regex, "");
    }

    //Method for checking date frames
    private List<User> getTimeFrame(Date after, Date before, List<User> users) {
        if(after == null && before == null) {
            return users;
        }

        Date afterMin = after == null? new Date(Long.MIN_VALUE) : after;
        Date beforeMax = before == null? new Date(Long.MAX_VALUE) : before;

        List<User> filteredByDateUsers = users.stream()
                .filter(user -> user.getDate().after(afterMin) && user.getDate().before(beforeMax))
                .collect(Collectors.toList());
        return filteredByDateUsers;
    }

    //Method for filtering ips by param (username, event, status)
    private Set<String> getFilteredByParam(Date after, Date before, String param, String paramValue) {
        List<User> filteredByDate = getTimeFrame(after, before, users);
        Set<User> filteredByParam = new HashSet<>();

            if(("name").equals(param)) {
                filteredByParam = filteredByDate.stream()
                        .filter(u -> paramValue.equals(u.getName()))
                        .collect(Collectors.toSet());
            } else if ("event".equals(param)) {
                filteredByParam = filteredByDate.stream()
                        .filter(u -> paramValue.equals(u.getEvent().name()))
                        .collect(Collectors.toSet());
            } else if ("status".equals(param)) {
                filteredByParam = filteredByDate.stream()
                        .filter(u -> paramValue.equals(u.getStatus().name()))
                        .collect(Collectors.toSet());
            }
        return ipFromUserToString(filteredByParam);
    }

    private Set<String> ipFromUserToString(Set<User> users) {
        Set<String> ipString = new HashSet<>();
        for(User user : users) {
            ipString.add(user.getIp());
        }
        return ipString;
    }
}