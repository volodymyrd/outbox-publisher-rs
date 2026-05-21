extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Lit, Type, TypePath};

/// Derive `DomainEvent` for a struct.
///
/// ## Required struct-level attributes
///
/// ```text
/// #[event(kind = "aggregate.action@vN")]
/// #[event(aggregate = "aggregate_type")]
/// ```
///
/// Or combined:
///
/// ```text
/// #[event(kind = "aggregate.action@vN", aggregate = "aggregate_type")]
/// ```
///
/// ## Required field-level attribute
///
/// Exactly one field must carry `#[event(aggregate_id)]`. That field must be
/// of type `Uuid`.
///
/// ## Example
///
/// ```rust
/// use outbox_publisher_derive::DomainEvent;
/// use outbox_publisher::domain_event::DomainEvent;
/// use uuid::Uuid;
///
/// #[derive(DomainEvent)]
/// #[event(kind = "user.registered@v1", aggregate = "user")]
/// pub struct UserRegistered {
///     #[event(aggregate_id)]
///     pub user_id: Uuid,
///     pub email: String,
/// }
///
/// let ev = UserRegistered { user_id: Uuid::nil(), email: "a@b.com".into() };
/// assert_eq!(UserRegistered::kind(), "user.registered@v1");
/// assert_eq!(UserRegistered::aggregate_type(), "user");
/// assert_eq!(ev.aggregate_id(), Uuid::nil());
/// ```
#[proc_macro_derive(DomainEvent, attributes(event))]
pub fn derive_domain_event(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    match expand_domain_event(ast) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_domain_event(ast: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    // Only structs are supported.
    let fields = match &ast.data {
        Data::Struct(ds) => &ds.fields,
        Data::Enum(_de) => {
            return Err(syn::Error::new_spanned(
                &ast.ident,
                "`#[derive(DomainEvent)]` can only be applied to structs, not enums",
            ));
        }
        Data::Union(du) => {
            return Err(syn::Error::new_spanned(
                du.union_token,
                "`#[derive(DomainEvent)]` can only be applied to structs, not unions",
            ));
        }
    };

    // Parse struct-level `#[event(...)]` attributes.
    let (kind, aggregate) = parse_struct_attrs(&ast)?;

    // Find the field marked `#[event(aggregate_id)]`.
    let agg_id_field = find_aggregate_id_field(&ast.ident, fields)?;

    let struct_ident = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::outbox_publisher::domain_event::DomainEvent
            for #struct_ident #ty_generics
            #where_clause
        {
            fn kind() -> &'static str where Self: ::std::marker::Sized {
                #kind
            }
            fn aggregate_type() -> &'static str where Self: ::std::marker::Sized {
                #aggregate
            }
            fn aggregate_id(&self) -> ::uuid::Uuid {
                self.#agg_id_field
            }
        }
    })
}

/// Parse `kind` and `aggregate` string literals from `#[event(...)]` on the struct.
fn parse_struct_attrs(ast: &DeriveInput) -> syn::Result<(String, String)> {
    let mut kind: Option<String> = None;
    let mut aggregate: Option<String> = None;

    for attr in &ast.attrs {
        if !attr.path().is_ident("event") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("kind") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = &lit {
                    if kind.is_some() {
                        return Err(meta.error("`kind` specified more than once"));
                    }
                    kind = Some(s.value());
                } else {
                    return Err(meta.error("`kind` must be a string literal"));
                }
            } else if meta.path.is_ident("aggregate") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = &lit {
                    if aggregate.is_some() {
                        return Err(meta.error("`aggregate` specified more than once"));
                    }
                    aggregate = Some(s.value());
                } else {
                    return Err(meta.error("`aggregate` must be a string literal"));
                }
            } else if meta.path.is_ident("aggregate_id") {
                return Err(
                    meta.error("`aggregate_id` is a field attribute, not a struct attribute")
                );
            } else {
                return Err(meta.error("unknown `#[event]` attribute key"));
            }
            Ok(())
        })?;
    }

    let kind_val = kind.ok_or_else(|| {
        syn::Error::new_spanned(
            &ast.ident,
            "missing required `#[event(kind = \"...\")]` attribute",
        )
    })?;
    let aggregate_val = aggregate.ok_or_else(|| {
        syn::Error::new_spanned(
            &ast.ident,
            "missing required `#[event(aggregate = \"...\")]` attribute",
        )
    })?;

    Ok((kind_val, aggregate_val))
}

/// Find exactly one field annotated `#[event(aggregate_id)]` and validate it is `Uuid`.
fn find_aggregate_id_field(
    struct_ident: &syn::Ident,
    fields: &Fields,
) -> syn::Result<proc_macro2::TokenStream> {
    let named = match fields {
        Fields::Named(f) => f,
        _ => {
            return Err(syn::Error::new_spanned(
                struct_ident,
                "`#[derive(DomainEvent)]` requires a struct with named fields",
            ));
        }
    };

    let mut found: Vec<(&syn::Field, proc_macro2::TokenStream)> = Vec::new();

    for field in &named.named {
        let mut field_marked = false;
        for attr in &field.attrs {
            if !attr.path().is_ident("event") {
                continue;
            }
            let mut is_aggregate_id = false;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("aggregate_id") {
                    is_aggregate_id = true;
                    Ok(())
                } else {
                    Err(meta.error(
                        "unknown `#[event]` key on field; only `aggregate_id` is valid here",
                    ))
                }
            })?;
            if is_aggregate_id {
                if field_marked {
                    return Err(syn::Error::new_spanned(
                        attr,
                        "`#[event(aggregate_id)]` specified more than once on the same field",
                    ));
                }
                field_marked = true;
                let ident = field
                    .ident
                    .as_ref()
                    .expect("Fields::Named guarantees field.ident is Some");
                found.push((field, quote!(#ident)));
            }
        }
    }

    match found.len() {
        0 => Err(syn::Error::new_spanned(
            struct_ident,
            "no field marked `#[event(aggregate_id)]`; exactly one `Uuid` field must be annotated",
        )),
        1 => {
            let (field, accessor) = &found[0];
            validate_uuid_field(field)?;
            Ok(accessor.clone())
        }
        _ => {
            // Point the error at the second occurrence.
            let (field, _) = &found[1];
            Err(syn::Error::new_spanned(
                field
                    .ident
                    .as_ref()
                    .expect("Fields::Named guarantees field.ident is Some"),
                "multiple fields marked `#[event(aggregate_id)]`; only one is allowed",
            ))
        }
    }
}

/// Return an error if the field type is not exactly `Uuid` or `uuid::Uuid`.
fn validate_uuid_field(field: &syn::Field) -> syn::Result<()> {
    if is_uuid_type(&field.ty) {
        return Ok(());
    }
    Err(syn::Error::new_spanned(
        &field.ty,
        "`#[event(aggregate_id)]` field must be of type `Uuid`",
    ))
}

fn is_uuid_type(ty: &Type) -> bool {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return false;
    };
    let segs: Vec<&syn::PathSegment> = path.segments.iter().collect();
    match segs.as_slice() {
        [last] => last.ident == "Uuid",
        [.., prev, last] => last.ident == "Uuid" && prev.ident == "uuid",
        _ => false,
    }
}
