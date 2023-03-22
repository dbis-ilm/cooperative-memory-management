#pragma once

/** A helper class to define CRTP class hierarchies.
 *
 * Taken from [An Implementation Helper For The Curiously Recurring Template Pattern - Fluent
 * C++](https://www.fluentcpp.com/2017/05/19/crtp-helper/).
 */
template<typename T, template<typename...> typename CrtpType, typename... Others>
struct CRTP
{
    using ActualType = T;
    ActualType& actual() { return *static_cast<ActualType*>(this); }
    const ActualType& actual() const { return *static_cast<const ActualType*>(this); }

private:
    CRTP() { }                                 // no one can construct this
    friend CrtpType<ActualType, Others...>;    // except classes that properly inherit from this class
};