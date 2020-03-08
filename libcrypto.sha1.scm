;;; openssl's SHA1 implementation. this is the one used by the sha1sum
;;; that we all know so well. it's about twice as fast as a
;;; sha1-c-source-code copy-paste on my system.
;;;
;;;
;;; example usage:
;;;
;;; (define ctx (init))
;;; (let ((buffer (make-string (* 4 1024 1024))))
;;;   (port-for-each (lambda (len) (update ctx buffer len))
;;;                  (lambda () (let ((len (read-string! #f buffer)))
;;;                               (if (zero? len) #!eof len)))))
;;; (print (final ctx))
(module libcrypto.sha1 (init update! final!)
(import scheme chicken.base)
(import chicken.foreign chicken.blob (only chicken.memory.representation number-of-bytes))

(foreign-declare "
#include <openssl/sha.h>
")

(define (check* ret) (unless (= 1 ret) (error "sha1 problem" ret 'sha1)))

(define (init)
  (let ((buf (make-blob (foreign-value "sizeof(SHA_CTX)" int))))
    (check* ((foreign-lambda int "SHA1_Init" scheme-pointer) buf))
    buf))

(define (update! ctx str #!optional len)
  ;;                                              ctx               buf        len
  (check* ( (foreign-lambda int "SHA1_Update" scheme-pointer scheme-pointer size_t )
            ctx str (or len (number-of-bytes str)))))

(define (final! ctx)
  (let ((buf (make-blob (foreign-value "SHA_DIGEST_LENGTH" int))))
    ;;                                       md             ctx
    ( (foreign-lambda int "SHA1_Final" scheme-pointer scheme-pointer)
      buf ctx)
    buf))

)
