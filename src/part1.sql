DROP TABLE IF EXISTS P2P;
DROP TABLE IF EXISTS Recommendations;
DROP TABLE IF EXISTS XP;
DROP TABLE IF EXISTS TimeTracking;
DROP TABLE IF EXISTS Friends;
DROP TABLE IF EXISTS TransferredPoints;
DROP TABLE IF EXISTS Verter;
DROP TABLE IF EXISTS Checks;
DROP TABLE IF EXISTS Tasks;
DROP TABLE IF EXISTS Peers;
DROP TYPE IF EXISTS CheckStatus;
CREATE TYPE CheckStatus AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE Peers
(
 Nickname VARCHAR NOT NULL PRIMARY KEY,
 Birthday DATE NOT NULL
);

CREATE TABLE Tasks
(
 Title VARCHAR NOT NULL PRIMARY KEY,
 ParentTask VARCHAR DEFAULT NULL,
 MaxXP INTEGER NOT NULL
);

CREATE TABLE Checks
(
 ID SERIAL PRIMARY KEY,
 Peer VARCHAR NOT NULL REFERENCES Peers,
 Task VARCHAR NOT NULL REFERENCES Tasks,
 "Date" DATE DEFAULT now() NOT NULL
);

CREATE TABLE P2P
(
 ID SERIAL PRIMARY KEY,
 "Check" BIGINT NOT NULL REFERENCES Checks,
 CheckingPeer VARCHAR NOT NULL REFERENCES Peers,
 "State" CheckStatus NOT NULL,
 "Time" TIME DEFAULT now() NOT NULL
);

CREATE TABLE Verter
(
 ID SERIAL PRIMARY KEY,
 "Check" BIGINT NOT NULL REFERENCES Checks,
 "State" CheckStatus NOT NULL,
 "Time" TIME DEFAULT now() NOT NULL
);

CREATE TABLE TransferredPoints
(
 ID SERIAL PRIMARY KEY,
 CheckingPeer VARCHAR NOT NULL REFERENCES Peers,
 CheckedPeer VARCHAR NOT NULL REFERENCES Peers,
 PointsAmount INTEGER DEFAULT 0
);

CREATE TABLE Friends
(
 ID SERIAL PRIMARY KEY,
 Peer1 VARCHAR NOT NULL,
 Peer2 VARCHAR NOT NULL,
 CONSTRAINT FK_Friends_Peer1_Peers_ID FOREIGN KEY (Peer1) REFERENCES Peers (Nickname),
 CONSTRAINT FK_Friends_Peer2_Peers_ID FOREIGN KEY (Peer2) REFERENCES Peers (Nickname),
 CONSTRAINT friends_peers_UQ UNIQUE (peer1, peer2),
 CONSTRAINT friends_peers_CH CHECK (peer1 <> peer2)
);

CREATE TABLE Recommendations
(
 ID SERIAL PRIMARY KEY,
 Peer VARCHAR NOT NULL,
 RecommendedPeer VARCHAR NOT NULL,
 CONSTRAINT FK_Friends_Peer_Peers_ID FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
 CONSTRAINT FK_Friends_RecommendedPeer_Peers_ID FOREIGN KEY (RecommendedPeer) REFERENCES Peers (Nickname),
 CONSTRAINT Recommendations_Peers_UQ UNIQUE (Peer, RecommendedPeer),
 CONSTRAINT Recommendations_Peers_CH CHECK (Peer <> RecommendedPeer)
);

CREATE TABLE XP
(
 ID SERIAL PRIMARY KEY,
 "Check" BIGINT NOT NULL REFERENCES Checks,
 XPAmount INTEGER
);

CREATE TABLE TimeTracking
(
 ID SERIAL PRIMARY KEY,
 Peer VARCHAR NOT NULL,
 "Date" DATE NOT NULL,
 "Time" TIME NOT NULL,
 "State" INTEGER NOT NULL,
 CONSTRAINT FK_TimeTracking_Peers_ID FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
 CONSTRAINT TimeTracking_State_CH CHECK ("State" = 1 OR "State" = 2)
);

CREATE OR REPLACE PROCEDURE from_csv(path text, separator char = ',')
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE ('COPY Peers (Nickname, Birthday) FROM '''|| path || '/peers.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Tasks (Title, ParentTask, MaxXP) FROM '''|| path ||'/tasks.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY Checks (Peer, Task, "Date") FROM '''|| path ||'/checks.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY Verter ("Check", "State", "Time") FROM '''|| path ||'/verter.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY XP ("Check", XPamount) FROM '''|| path ||'/xp.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY P2P ("Check", CheckingPeer, "State", "Time") FROM '''|| path ||'/p2p.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY TransferredPoints (CheckingPeer, CheckedPeer, PointsAmount) FROM '''|| path || '/transferred_points.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Friends (Peer1, Peer2) FROM '''|| path || '/friends.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Recommendations (Peer, RecommendedPeer) FROM '''|| path || '/recommendations.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY TimeTracking (Peer, "Date", "Time", "State") FROM '''|| path || '/timetracking.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
END
$$;

CREATE OR REPLACE PROCEDURE to_csv(path text, separator char = ',')
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE ('COPY Peers (Nickname, Birthday) TO '''|| path || '/peers.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Tasks (Title, ParentTask, MaxXP) TO '''|| path ||'/tasks.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY Checks (Peer, Task, "Date") TO '''|| path ||'/checks.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY Verter ("Check", State, "Time") TO '''|| path ||'/verter.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY XP ("Check", XPamount) TO '''|| path ||'/xp.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY P2P ("Check", CheckingPeer, State, "Time") TO '''|| path ||'/p2p.csv'' WITH (FORMAT CSV, DELIMITER ''' || separator || ''')');
    EXECUTE ('COPY TransferredPoints (CheckingPeer, CheckedPeer, PointsAmount) TO '''|| path || '/transferred_points.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Friends (Peer1, Peer2) TO '''|| path || '/friends.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY Recommendations (Peer, RecommendedPeer) TO '''|| path || '/recommendations.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
    EXECUTE ('COPY TimeTracking (Peer, "Date", "Time", State) TO '''|| path || '/timetracking.csv'' WITH (FORMAT CSV, DELIMITER '''|| separator || ''')');
END
$$;

CALL from_csv('/Users/natalia/Desktop/SQL2_Info21_v1.0-2/src/csv', ',');