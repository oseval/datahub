package ru.oseval.datahub

import ru.oseval.datahub.data.ACIDataOps;

public class UserOps extends ACIDataOps<User> {
    public static class User {
        private int userId;
        private String name;
        public User(int userId, String name) {
            this.userId = userId;
            this.name = name;
        }
    }
}
