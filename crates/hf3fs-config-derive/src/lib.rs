use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Expr, Fields, Lit, Type};

/// Attributes parsed from `#[config(...)]` on a field.
struct ConfigFieldAttrs {
    default: Option<TokenStream2>,
    hot_updated: bool,
    section: bool,
    min: Option<TokenStream2>,
    max: Option<TokenStream2>,
    doc: Option<String>,
}

fn parse_config_attrs(attrs: &[syn::Attribute]) -> ConfigFieldAttrs {
    let mut result = ConfigFieldAttrs {
        default: None,
        hot_updated: false,
        section: false,
        min: None,
        max: None,
        doc: None,
    };

    for attr in attrs {
        if !attr.path().is_ident("config") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("hot_updated") {
                result.hot_updated = true;
                return Ok(());
            }
            if meta.path.is_ident("section") {
                result.section = true;
                return Ok(());
            }
            if meta.path.is_ident("default") {
                let value = meta.value()?;
                let expr: Expr = value.parse()?;
                result.default = Some(quote!(#expr));
                return Ok(());
            }
            if meta.path.is_ident("min") {
                let value = meta.value()?;
                let expr: Expr = value.parse()?;
                result.min = Some(quote!(#expr));
                return Ok(());
            }
            if meta.path.is_ident("max") {
                let value = meta.value()?;
                let expr: Expr = value.parse()?;
                result.max = Some(quote!(#expr));
                return Ok(());
            }
            if meta.path.is_ident("doc") {
                let value = meta.value()?;
                let lit: Lit = value.parse()?;
                if let Lit::Str(s) = lit {
                    result.doc = Some(s.value());
                }
                return Ok(());
            }
            Err(meta.error("unrecognized config attribute"))
        })
        .expect("failed to parse #[config(...)] attribute");
    }

    result
}

/// Returns true if the type is `String`.
fn is_string_type(ty: &Type) -> bool {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            return seg.ident == "String";
        }
    }
    false
}

/// Returns true if the type is `bool`.
fn is_bool_type(ty: &Type) -> bool {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            return seg.ident == "bool";
        }
    }
    false
}

#[proc_macro_derive(Config, attributes(config))]
pub fn derive_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let fields = match &input.data {
        Data::Struct(ds) => match &ds.fields {
            Fields::Named(named) => &named.named,
            _ => panic!("Config derive only supports structs with named fields"),
        },
        _ => panic!("Config derive only supports structs"),
    };

    let mut default_fields = Vec::new();
    let mut from_toml_fields = Vec::new();
    let mut hot_update_fields = Vec::new();
    let mut render_fields = Vec::new();
    let mut validate_fields = Vec::new();

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();
        let field_ty = &field.ty;
        let attrs = parse_config_attrs(&field.attrs);

        // -- Default impl --
        if attrs.section {
            default_fields.push(quote! {
                #field_name: <#field_ty as Default>::default(),
            });
        } else if let Some(ref default_expr) = attrs.default {
            if is_string_type(field_ty) {
                default_fields.push(quote! {
                    #field_name: (#default_expr).to_string(),
                });
            } else {
                default_fields.push(quote! {
                    #field_name: #default_expr,
                });
            }
        } else {
            panic!(
                "Field `{}` must have either #[config(default = ...)] or #[config(section)]",
                field_name_str
            );
        }

        // -- from_toml --
        if attrs.section {
            from_toml_fields.push(quote! {
                #field_name: if let Some(sub) = table.get(#field_name_str) {
                    <#field_ty as hf3fs_config::Config>::from_toml(sub)?
                } else {
                    <#field_ty as Default>::default()
                },
            });
        } else if is_string_type(field_ty) {
            let default_expr = attrs.default.as_ref().expect("non-section field needs default");
            from_toml_fields.push(quote! {
                #field_name: if let Some(v) = table.get(#field_name_str) {
                    v.as_str()
                        .ok_or_else(|| hf3fs_config::ConfigError::TypeMismatch {
                            field: #field_name_str.into(),
                            expected: "string".into(),
                        })?
                        .to_string()
                } else {
                    (#default_expr).to_string()
                },
            });
        } else if is_bool_type(field_ty) {
            let default_expr = attrs.default.as_ref().expect("non-section field needs default");
            from_toml_fields.push(quote! {
                #field_name: if let Some(v) = table.get(#field_name_str) {
                    v.as_bool()
                        .ok_or_else(|| hf3fs_config::ConfigError::TypeMismatch {
                            field: #field_name_str.into(),
                            expected: "bool".into(),
                        })?
                } else {
                    #default_expr
                },
            });
        } else {
            // Numeric type (i64, u64, f64, etc.) -- parse as i64 from toml
            let default_expr = attrs.default.as_ref().expect("non-section field needs default");
            from_toml_fields.push(quote! {
                #field_name: if let Some(v) = table.get(#field_name_str) {
                    v.as_integer()
                        .map(|n| n as #field_ty)
                        .or_else(|| v.as_float().map(|f| f as #field_ty))
                        .ok_or_else(|| hf3fs_config::ConfigError::TypeMismatch {
                            field: #field_name_str.into(),
                            expected: "number".into(),
                        })?
                } else {
                    #default_expr
                },
            });
        }

        // -- hot_update --
        if attrs.section {
            hot_update_fields.push(quote! {
                hf3fs_config::Config::hot_update(&mut self.#field_name, &other.#field_name);
            });
        } else if attrs.hot_updated {
            hot_update_fields.push(quote! {
                self.#field_name = other.#field_name.clone();
            });
        }

        // -- render --
        if attrs.section {
            render_fields.push(quote! {
                out.push_str(&format!("[{}]\n", #field_name_str));
                out.push_str(&hf3fs_config::Config::render(&self.#field_name));
                out.push('\n');
            });
        } else if is_string_type(field_ty) {
            render_fields.push(quote! {
                out.push_str(&format!("{} = \"{}\"\n", #field_name_str, self.#field_name));
            });
        } else if is_bool_type(field_ty) {
            render_fields.push(quote! {
                out.push_str(&format!("{} = {}\n", #field_name_str, self.#field_name));
            });
        } else {
            render_fields.push(quote! {
                out.push_str(&format!("{} = {}\n", #field_name_str, self.#field_name));
            });
        }

        // -- validate --
        if attrs.section {
            validate_fields.push(quote! {
                hf3fs_config::Config::validate(&self.#field_name)?;
            });
        }
        if let Some(ref min_expr) = attrs.min {
            validate_fields.push(quote! {
                if (self.#field_name as i64) < (#min_expr as i64) {
                    return Err(hf3fs_config::ConfigError::OutOfRange {
                        field: #field_name_str.into(),
                        value: format!("{}", self.#field_name),
                        min: Some(format!("{}", #min_expr)),
                        max: None,
                    });
                }
            });
        }
        if let Some(ref max_expr) = attrs.max {
            validate_fields.push(quote! {
                if (self.#field_name as i64) > (#max_expr as i64) {
                    return Err(hf3fs_config::ConfigError::OutOfRange {
                        field: #field_name_str.into(),
                        value: format!("{}", self.#field_name),
                        min: None,
                        max: Some(format!("{}", #max_expr)),
                    });
                }
            });
        }
    }

    let expanded = quote! {
        impl #impl_generics Default for #name #ty_generics #where_clause {
            fn default() -> Self {
                Self {
                    #(#default_fields)*
                }
            }
        }

        impl #impl_generics hf3fs_config::Config for #name #ty_generics #where_clause {
            fn from_toml(value: &toml::Value) -> Result<Self, hf3fs_config::ConfigError> {
                let table = value;
                Ok(Self {
                    #(#from_toml_fields)*
                })
            }

            fn hot_update(&mut self, other: &Self) {
                #(#hot_update_fields)*
            }

            fn render(&self) -> String {
                let mut out = String::new();
                #(#render_fields)*
                out
            }

            fn validate(&self) -> Result<(), hf3fs_config::ConfigError> {
                #(#validate_fields)*
                Ok(())
            }
        }
    };

    TokenStream::from(expanded)
}
