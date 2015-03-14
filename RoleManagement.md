# Introduction #

oak-roles-manager adds support for role management: the creation of roles and assigning accounts to those roles


# Details #

The `mysql.user` table has three columns of interest:
User, Host, Password
There is a PRIMARY KEY on (User, Host)

oak-roles-manager wishes to impose roles using the very same `mysql.user` table, even though it leads to denormalized data. The reasoning is to utilize existing tables within the mysql schema, which can then be exported natively.
The addition of a dedicated schema, while may seem attractive, is abandoned.

## Roles ##
A role is a group, into which accounts can be assigned. A role has privileges, just like any normal mysql account.
In fact, as far as MySQL is concerned, a role IS a normal account.

A role has a distinct name. Role names are mapped to user names in mysql. That is, the name of the role appears in the `User` column in the `mysql.user` table.
This explains how MySQL treats roles as normal accounts.
Each role is also associated with a unique two characters code, e.g. "4s", or "gK"
A role mapped to a MySQL account (henceforth known as role-account) must be somehow distinguished from a normal account. It is also required that this account cannot be used directly.
To solve this, the role is assigned with a non-workable password (`Password` column in `mysql.user` table). A non-workable password is one which is valid as far as MySQL is concerned, but which can never login.
A workable password in MySQL is a hashed text which is (in old-passsswords=1) a 16 hexadecimal characters, or (in old-passwords=0) a "**" followed by 40 hexadecimal characters.
It follows that any text which contains non-hexadecimal characters can never be a hashed password.
Therefore, the password for a role-account is always "oak:role:account:XX:#####################" - 41 characters in length, where XX is the role's unique identifier.**

So, A role entry would be something like (user, host, password):
('webuser-role', '**bd72562cad6d032efe', 'oak:role:account:####################')**

Normal accounts can be associated with zero or more roles. The associations are also mapped to the `mysql.user` table.
An account-role association is presented in the following manner:
`User` is same as account's `User`
`Host` is account's `Host`, followed by ":XX", where XX is the role's unique identifier.
`Password` is "oak:role:association:<role name>:###...####"



--fix: iterates all accounts with roles, REVOKEs everything an GRANTs again based on roles (in case someone changed GRANTs for roles manually etc.)
--create-role=

&lt;name&gt;


--drop-role=

&lt;name&gt;


--assign-role=

&lt;name&gt;

 --account=<user@host>
or maybe:
--action=create-role --role=

&lt;name&gt;


--action=assign --role=

&lt;name&gt;

 --account=<user@host> [--no-auto-create] // creates account if not exists
--action=detach --role=

&lt;name&gt;

 --account=<user@host> [--no-auto-drop] // drops if indeed belonged to role and now has zero roles
--action=grant --role=

&lt;name&gt;

 --grant="SELECT, INSERT ON **.**"