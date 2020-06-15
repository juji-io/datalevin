var datalevin = {};
datalevin.db = {};

/**
 * @constructor
 */
datalevin.db.Datom = function() {};
datalevin.db.Datom.prototype.e;
datalevin.db.Datom.prototype.a;
datalevin.db.Datom.prototype.v;
datalevin.db.Datom.prototype.tx;


datalevin.impl = {};
datalevin.impl.entity = {};

/**
 * @constructor
 */
datalevin.impl.entity.Entity = function() {};
datalevin.impl.entity.Entity.prototype.db;
datalevin.impl.entity.Entity.prototype.eid;
datalevin.impl.entity.Entity.prototype.keys      = function() {};
datalevin.impl.entity.Entity.prototype.entries   = function() {};
datalevin.impl.entity.Entity.prototype.values    = function() {};
datalevin.impl.entity.Entity.prototype.has       = function() {};
datalevin.impl.entity.Entity.prototype.get       = function() {};
datalevin.impl.entity.Entity.prototype.forEach   = function() {};
datalevin.impl.entity.Entity.prototype.key_set   = function() {};
datalevin.impl.entity.Entity.prototype.entry_set = function() {};
datalevin.impl.entity.Entity.prototype.value_set = function() {};
