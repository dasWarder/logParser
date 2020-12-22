package com.javarush.task.task39.task3913;

import com.javarush.task.task39.task3913.query.IPQuery;
import com.javarush.task.task39.task3913.query.UserQuery;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LogParser implements IPQuery, UserQuery {

    private class User {
        private String ip;
        private String name;
        private Date date;
        private Event event;
        private Integer code;
        private Status status;

        public User() {};
        public User(String ip, String name, Date date, Event event, Integer code, Status status) {
            this.ip = ip;
            this.name = name;
            this.date = date;
            this.event = event;
            this.code = code;
            this.status = status;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(Date date) {
            this.date = date;
        }

        public Event getEvent() {
            return event;
        }

        public void setEvent(Event event) {
            this.event = event;
        }

        public Integer getCode() {
            return code;
        }

        public void setCode(Integer code) {
            this.code = code;
        }

        public Status getStatus() {
            return status;
        }

        public void setStatus(Status status) {
            this.status = status;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            User user = (User) o;
            return Objects.equals(ip, user.ip) &&
                    Objects.equals(name, user.name) &&
                    Objects.equals(date, user.date) &&
                    event == user.event &&
                    Objects.equals(code, user.code) &&
                    status == user.status;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip, name, date, event, code, status);
        }

        @Override
        public String toString() {
            return "User{" +
                    "ip='" + ip + '\'' +
                    ", name='" + name + '\'' +
                    ", date=" + date +
                    ", event=" + event +
                    ", code=" + code +
                    ", status=" + status +
                    '}';
        }
    }

    private List<User> users;
    private List<String> allStrings;
    private final Function<User, String> BY_NAME = u -> u.getName();

    @Override
    public Set<String> getAllUsers() {
        return filteredByPredicate(u ->true, u -> true, BY_NAME, null, null);
    }

    @Override
    public int getNumberOfUsers(Date after, Date before) {
        return filteredByPredicate(user -> true, user -> true, BY_NAME, after, before).size();
    }

    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {
        Predicate<User> filteredByEvent = u -> u.getName().equals(user);
        Function<User, Event> toEvent = u-> u.getEvent();

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
        Predicate<User> filteredByPlugin = u->u.getEvent() == Event.DOWNLOAD_PLUGIN;

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

    private <T> Set<String> filteredByPredicate(Predicate<User> predicate, Predicate<User> optionalPredicate, Function<User, T> function, Date after, Date before) {
        List<User> filteredByDate = getTimeFrame(after, before, users);
        return (Set<String>) filteredByDate.stream()
                .filter(predicate)
                .filter(optionalPredicate)
                .map(function)
                .collect(Collectors.toSet());
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