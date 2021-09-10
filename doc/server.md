# Datalevin Server/Client

## Usage

Using the same native command line tool, `dtlv serv` will run in the server mode
and accepts network connection on port 8898 (default).

* Use option `-p` to specify an alternative port number that the server listens
  to. Proper firewall settings is needed to allow remote access to the port.
* `-r` option can be used to specify a root directory path on the server, where
  all data reside under. The default path is `/var/lib/datalevin` on Posix
  systems, `C:\ProgramData\Datalevin` on Windows. User should
  make sure read/write file permissions are set on the directory path for the
  user running the server.
* `-v` option enables verbose server debug logs. Datalevin server writes logs to
  stdout.

There is a default builtin user `datalevin` with a default password `datalevin`.
This is a system account that can do everything on the server. It
is recommended that the default password should be reset immediately after
installation:

1. Start the server, maybe as a sudo user, to access the default data root directory
```console
# dtlv serv
```
2. Start Datalevin REPL in another terminal:

```console
$ dtlv
```

3. Type the following in the REPL:

```console
user> (def client (new-client "dtlv://datalevin:datalevin@localhost"))
#'user/client
user> (reset-password client "datalevin" "new-password")
nil
```

It is suggested to create different users for access to the server (see below).
Leave the `datalevin` user for server administration purpose only.

The user is recommended to run the server process as a daemon or service using
the preferred operation system tools, e.g. systemd on Linux, Launch Daemon on
MacOS, or sc.exe on Windows. Packagers are welcomed to package Datalevin server
on the preferred platforms.

For remote access, username and password is required on the connection URI.
When a client (for now, just the Datalevin library itself) opens a Datalevin database
using a connection URI, i.e.
"dtlv://&lt;username&gt;:&lt;password&gt;@&lt;hostname&gt;:&lt;port&gt;/&lt;db-name&gt;?store=datalog|kv",
instead of a local path name, a connection to the server is attempted. `db-name`
should be unique on the server. `store` parameter is optional, default is
`datalog`. A database will be created if it does not yet exist.

The same functions for local databases work on the remote databases. The remote
access is transparent to function callers.

## Implementation

The client/server mode is enabled with little changes to the Datalevin core library.

### Architecture

As mentioned, the main design criteria is to have transparent remote databases
access that has the same API as local databases. This is achieved by building a
remote access layer to proxy the database storage. Each local storage access function
has a corresponding proxy function that performs remote storage access over the
wire. That is to say, the local and remote storage present exactly the same
interface to the higher level callers.

Compared with traditional client/server architecture, where the server performs
all the actual data processing work, the architecture of Datalevin enable easier
implementation of rich user convenience features. For much of the high level
functionalities sit on top of storage, such as caching, transaction data
preparation, query parse, change listening, and so on, they are handled on
the client side, which is the same as in the local embedded mode. For example,
our recent added feature of [transactable
entity](https://github.com/juji-io/datalevin#entities-with-staged-transactions-datalog-store) works the same in
either embedded or server mode, without needing any code changes.

Compared with the peer architecture of Datomic®, where peers receive all the
data, Datalevin clients requests only the needed data on demand. The amount of
network traffic is reduced, clients are simpler than peers and have less work to
do, so the impact on the user application performance is minimized. Because not
all the data are duplicated on all the peers, the size of the database only
depends on the capacity of the server, which can afford to be a beefy machine.

In Datalevin client/server mode, transaction and querying can happen both in
client and server side, depending on the context. For example, for queries
having a single remote data source, the entire query processing is done remotely
to save networking traffic. For other cases, only low level data access
functions are handled on the server.

Each client always check `last-modified` time of the remote database before data
access, so when multiple clients are accessing the same database on the server,
all see the same most update-to-date data, as long as the clients and the server
have clock synchronization, which is a mild condition that most modern server
deployment environment should meet, with ntp or chrony services being part of
the standard server environment. In term of CAP theorem, Datalevin favors
consistency over availability, in consistent with our goal of simplifying data access.

All these are transparent to the users and the same data access API works for
all cases.  Further optimizations can be implemented behind the scene
without having to introduce new operational complexities.

### Networking

The server employs a non-blocking event driven architecture, so it can support a
large number of concurrent connected clients. The server event loop runs as a
single process. It accepts and segments incoming bytes from the network into
messages, then dispatches them to a work stealing thread pool to handle each
individual message.

Work stealing thread pool reduces lock contentions and maximizes the server CPU
utilization. Each thread processes its message and writes its own response back
to the network channel when it becomes ready, so the server message handling is
asynchronous. It is the client's responsibility to track request/response
correspondence if multiple messages are on the wire.

For developer convenience, the current implemented client in the library makes
synchronous and blocking network connections. For normal commands, it sends a
request and waits for the responses from the server, so the data access API is
the same for both the local databases and remote databases. In addition, the
client has a built-in connection pool, to reuse pre-established connections.

The wire protocol between server and client is largely inspired by the wire
protocol of PostgreSQL. It uses TLV message format, with 1 byte message type in
front, followed by 4 bytes message length, and concludes with the message
payload.

The payload format is extensible, indicated by
the message type byte. For example, with type `1`,
[transit+json](https://github.com/cognitect/transit-format) encoded bytes will
be the payload. The default payload format is type `2`, using
[nippy](https://github.com/ptaoussanis/nippy) serialization.

nippy format produces smaller bytes with faster speed, but it only works
with Clojure code. If a client needs to be written for other languages, transit
is a better choice. The server accepts either format just as well. Other format
may be added in the future if necessary.

The command messages are EDN maps, e.g. `{:type :list-databases :args []}`. The
command responses are also EDN maps. e.g. `{:type :command-complete :results
["mydb" "hr-db"]}`. For bulk data, the client/server switch to a direct
copy-in/copy-out sub-protocol, where data are continuously streamed. The
copy-in/copy-out data stream messages are batched data in EDN vectors
instead of maps.

User defined functions (e.g. filtering predicates) are serialized and sent to
server for execution. They are first evaluated in the sandbox using a Clojure
interpreter, i.e. [sci](https://github.com/borkdude/sci) based on a white list.
Once interpreted, they become the same kind of Clojure functions as if compiled,
so the performance hit is minimal. It is also more secure, as there's less
danger of malicious user code bringing down the server.

### Security

Datalevin server implements full-fledged role based access control (RBAC).
Permissions are granted to roles, and roles are assigned to users. User access
is secured by password.

A permission consists of three pieces of information:

* `:permission/act` indicates the permitted actions, and it can be one of
  `:datalevin.server/view`, `:datalevin.server/alter`,
  `:datalevin.server/create`, or `:datalevin.server/control`, in increasing
  level of privilege, and the latter implies the former.
* `:permission/obj` indicates the object type of the securable, and it can be
  one of `:datalevin.server/user`, `:datalevin.server/role`,
  `:datalevin.server/database`, or `:datalevin.server/server`, with the last one
  implies all others.
* `:permission/tgt` refers to the concrete target of the securable. It could be
  a username, a role keyword, a database name or `nil`, depending on
  `permission/obj`. All these target names uniquely identify securable objects.
  When the target is `nil`, the permission applies to all objects of that type.


Each user has a corresponding built-in unique role, with a role keyword
`:datalevin.role/<username>`. For example, the default user `datalevin`  has a
built-in role `:datalevin.role/datalevin`. This role is granted the permission
`{:permission/act :datalevin.server/control, :permission/obj
:datalevin.server/server}`, which permits the role to do everything on the
server.

In the command line REPL, after connecting to a server, issue [`(create-user
...)`](https://juji-io.github.io/datalevin/datalevin.client.html#var-create-user) to create a user, [`(create-role ...)`](https://juji-io.github.io/datalevin/datalevin.client.html#var-create-role) to create a role, [`(assign-role
...)`](https://juji-io.github.io/datalevin/datalevin.client.html#var-assign-role) to assign a role to a user, [`(grant-permission ...)`](https://juji-io.github.io/datalevin/datalevin.client.html#var-grant-permission) to grant a permission
to a role.

User password is stored as a salt and a hash. The password hashing algorithm
takes the recommended more than 0.5 seconds to run on a modern server class
machine, so it can defeat a brutal force cracking effort.
