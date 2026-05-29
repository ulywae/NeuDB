
#ifndef DB_TOKEN_H
#define DB_TOKEN_H

#if !defined NEU_CORE_ECO_SYSTEM
/**
 * @def NEU_CORE_ECO_SYSTEM
 * @brief Global security token enabling private component compilation.
 * @details When defined, this macro signals to all internal header files
 *          that the request has successfully passed through the official
 *          gateway and is authorized to access core structures.
 */
#define NEU_CORE_ECO_SYSTEM
#endif

#endif // DB_TOKEN_H