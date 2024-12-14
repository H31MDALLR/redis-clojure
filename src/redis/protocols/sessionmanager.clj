(ns redis.protocols.sessionmanager)


(defprotocol SessionManager
  (expire! [this id])
  (get-or-create! [this fingerprint])
  (add-item! [this id path v])
  (delete-item! [this id path])
  (get-item! [this id path])
  (update-item! [this id path f]))
